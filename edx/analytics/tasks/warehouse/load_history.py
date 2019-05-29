"""Create history tables from Hive-imported tables."""
import logging
import textwrap

import luigi

from edx.analytics.tasks.common.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.insights.database_imports import (
    DatabaseImportMixin,
    ImportProductCatalog,
    ImportProductCatalogClass,
    ImportStudentCourseEnrollmentTask,
)

from edx.analytics.tasks.util.hive import hive_database_name, HivePartition, HiveTableTask, WarehouseMixin
from edx.analytics.tasks.util.url import get_target_from_url

log = logging.getLogger(__name__)


class BaseHiveHistoryTask(DatabaseImportMixin, HiveTableTask):
    # otto_credentials = luigi.Parameter(
    #     config_path={'section': 'otto-database-import', 'name': 'credentials'}
    # )
    # otto_database = luigi.Parameter(
    #     config_path={'section': 'otto-database-import', 'name': 'database'}
    # )

    source_task = None

    @property
    def source_import_class(self):
        """The class object that loads the source table into Hive."""
        raise NotImplementedError

    def requires(self):
        if self.source_task is None:
            kwargs = {
                'destination': self.destination,
                'num_mappers': self.num_mappers,
                'verbose': self.verbose,
                'import_date': self.import_date,
                'overwrite': self.overwrite,
                # 'credentials': self.otto_credentials,
                # 'database': self.otto_database,
                # Use LMS credentials instead
                'credentials': self.credentials,
                'database': self.database,
            }
            source_task = self.source_import_class(**kwargs)
        return source_task

    def get_source_task(self):
        return self.requires()
    
    @property
    def source_table(self):
        return self.get_source_task().table_name

    @property
    def source_columns(self):
        return self.get_source_task().columns

    @property
    def source_column_names(self):
        return [name for (name, type) in self.source_columns]

    @property
    def table(self):
        """The name of the output table in Hive, based on the source table."""
        return "{name}_history".format(name=self.source_table)

    @property
    def columns(self):
        column_names = self.source_column_names
        if column_names[0] <> 'id':
            raise Exception("first column in source columns is not named 'id'")
        
        # Map everything to a STRING, except the 'id' field, and add standard history fields.
        columns = [('id', 'INT'),]
        columns.extend([(name, 'STRING') for name in column_names[1:]])
        columns.extend([
            ('history_id', 'STRING'),
            ('history_date', 'STRING'),
            ('history_change_reason', 'STRING'),
            ('history_type', 'STRING'),
            ('history_user_id', 'STRING'),
        ])
        return columns
        
    @property
    def partition(self):
        # TODO: import_date is not necessarily the right location for output, given that it can be any date
        # in the past, as long as it exists (so we don't reload it).  Only if it's today or yesterday might
        # this better match the set of dates that are available, by roughly being the latest date.
        # On the other hand, maybe we could calculate that based on files in S3, and set import_date
        # appropriately to whatever exists.
        return HivePartition('dt', self.import_date.isoformat())  # pylint: disable=no-member

    @property
    def load_input_table_query(self):
        # Make sure that all tables are loaded from the input table.
        query = "MSCK REPAIR TABLE {source_table};".format(source_table=self.source_table)
        log.info('load_input_table_query: %s', query)
        return query

    @property
    def create_table_query(self):
        # Ensure there is exactly one available partition in the output table.
        query_format = """
            USE {database_name};
            DROP TABLE IF EXISTS `{table}`;
            CREATE EXTERNAL TABLE `{table}` (
                {col_spec}
            )
            PARTITIONED BY (`{partition.key}` STRING)
            {table_format}
            LOCATION '{location}';
            ALTER TABLE `{table}` ADD PARTITION ({partition.query_spec});
        """
        query = query_format.format(
            database_name=hive_database_name(),
            table=self.table,
            col_spec=','.join(['`{}` {}'.format(name, col_type) for name, col_type in self.columns]),
            location=self.table_location,
            table_format=self.table_format,
            partition=self.partition,
        )
        query = textwrap.dedent(query)
        log.info('create_table_query: %s', query)
        return query
    
    def query(self):
        full_insert_query = """
            INSERT INTO TABLE `{table}`
            PARTITION ({partition.query_spec})
            {insert_query}
        """.format(
            table=self.table,
            partition=self.partition,
            insert_query=self.insert_query.strip(),  # pylint: disable=no-member
        )

        full_query = self.create_table_query + self.load_input_table_query + textwrap.dedent(full_insert_query)
        log.info('History-creating hive query: %s', full_query)
        return full_query

    def output(self):
        # This is magically copied from HiveTableFromQueryTask.
        return get_target_from_url(self.partition_location.rstrip('/') + '/')

    def intermediate_columns(self):
        # BOOLEAN types in Hive have to be converted explicitly, because they cannot be coerced into Strings.  (Same with BINARY, FWIW.)
        coalesce_pairs = list()
        for (name, type) in self.source_columns:
            if type.lower() == 'boolean':
                pair = "COALESCE(IF({name},'1','0'),'NNULLL') AS {name}, COALESCE(IF(LAG({name}) OVER w,'1','0'),'NNULLL') AS lag_{name}".format(name=name)
            else:
                pair = "COALESCE({name},'NNULLL') AS {name}, COALESCE(LAG({name}) OVER w,'NNULLL') AS lag_{name}".format(name=name)
            coalesce_pairs.append(pair)

        return ', '.join(coalesce_pairs)

    @property
    def insert_query(self):
        # Convert back from the NULL marker to return actual null values, but skip the first (id) column.
        input_columns = ', '.join(["IF(t.{name} = 'NNULLL', NULL, t.{name}) AS {name}".format(name=name) for name in self.source_column_names[1:]])
        # For intermediate work, convert NULLs to NULL marker.
        intermediate_columns = self.intermediate_columns()
        where_clause = " OR ".join(["t.{name} <> t.lag_{name}".format(name=name) for name in self.source_column_names[1:]])
        query = """
        SELECT id, {input_columns},
            NULL AS history_id, 
            dt AS history_date, 
            NULL AS history_change_reason,
            IF(t.lag_dt = 'NNULLL', '+', '~') AS history_type, 
            NULL AS history_user_id
        FROM (
            SELECT 
                {intermediate_columns},
                COALESCE(dt,'NNULLL') AS dt, COALESCE(LAG(dt) OVER w,'NNULLL') AS lag_dt
            FROM {source_table}
            WINDOW w AS (PARTITION BY id ORDER BY dt)
        ) t
        WHERE {where_clause}
        """.format(
            input_columns=input_columns,
            intermediate_columns=intermediate_columns,
            source_table=self.source_table,
            where_clause=where_clause,
        )
        return query


class LoadHiveHistoryToWarehouse(WarehouseMixin, VerticaCopyTask):
    
    date = luigi.DateParameter()
    # n_reduce_tasks = luigi.Parameter()

    source_task = None

    @property
    def source_import_class(self):
        """The class object that loads the source table into Hive."""
        raise NotImplementedError

    @property
    def table(self):
        return self.insert_source_task.table

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None

    @property
    def auto_primary_key(self):
        """No automatic primary key here."""
        return None

    @property
    def columns(self):
        # convert Hive columns to Vertica columns.
        source_columns = self.insert_source_task.columns
        columns = [(name, 'INT' if type=='INT' else 'VARCHAR') for (name, type) in source_columns]
        return columns

    @property
    def partition(self):
        # TODO: is this supposed to match the source location?  Then get it from there?
        """The table is partitioned by date."""
        # return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member
        return self.insert_source_task.partition

    @property
    def insert_source_task(self):
        if self.source_task is None:
            kwargs = {
                # 'schema': self.schema,
                # 'marker_schema': self.marker_schema,
                # 'credentials': self.credentials,
                'destination': self.warehouse_path,
                'overwrite': self.overwrite,
                'warehouse_path': self.warehouse_path,
                'import_date': self.date,
            }
            self.source_task = self.source_import_class(**kwargs)
        return self.source_task        


class ProductCatalogClassHiveHistoryTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportProductCatalogClass


class LoadProductCatalogClassHiveHistoryToWarehouse(LoadHiveHistoryToWarehouse):    

    @property
    def source_import_class(self):
        return ProductCatalogClassHiveHistoryTask
    

class ProductCatalogHiveHistoryTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportProductCatalog


class LoadProductCatalogHiveHistoryToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return ProductCatalogHiveHistoryTask


class StudentCourseEnrollmentHiveHistoryTask(BaseHiveHistoryTask):

    @property
    def source_import_class(self):
        return ImportStudentCourseEnrollmentTask


class LoadStudentCourseEnrollmentHiveHistoryToWarehouse(LoadHiveHistoryToWarehouse):

    @property
    def source_import_class(self):
        return StudentCourseEnrollmentHiveHistoryTask
