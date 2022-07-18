import luigi
from luigi import Task, LocalTarget
from luigi.contrib import sqla
from pkg_resources import yield_lines
from sqlalchemy import String, Float


class DownloadFranceSales(Task):
    def output(self):
        return LocalTarget('My-First-Data-Pipeline/csv-to-sqlite/france.csv')

    def run(self):
        with self.output().open('w') as f:
            print('May, 3453', file=f)
            print('June, 1560', file=f)


class DownloadGermanySales(Task):
    def output(self):
        return LocalTarget('My-First-Data-Pipeline/csv-to-sqlite/germany.csv')

    def run(self):
        with self.output().open('w') as f:
            print('May, 123', file=f)
            print('June, 560', file=f)


class CreateData(sqla.CopyToTable):
    columns = [
        (["month", String(64)], {}),
        (["amount", Float], {})
    ]
    connection_string = "sqlite:////test.db"  # in memory SQLite database
    table = "sales"  # name of the table to store data
    column_separator = ','

    def rows(self):
        with self.input()[0].open() as f:
            for line in f:
                yield line.split(self.column_separator)
        with self.input()[1].open() as f:
            for line in f:
                yield line.split(self.column_separator)

    def requires(self):
        return [DownloadFranceSales(),
                DownloadGermanySales()]


# columns defines the table schema, with each element corresponding
# to a column in the format (args, kwargs) which will be sent to
# the sqlalchemy.Column(*args, **kwargs)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    luigi.run(['CreateData', '--local-scheduler'])
