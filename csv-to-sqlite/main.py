import luigi
from luigi import Task, LocalTarget
from luigi.contrib import sqla
from sqlalchemy import String, Float


class DownloadFranceSales(Task):
    def output(self):
        return LocalTarget('france.csv')

    def run(self):
        with self.output().open('w') as f:
            print('May, 3453', file=f)
            print('June, 1560', file=f)


class DownloadGermanySales(Task):
    def output(self):
        return LocalTarget('germany.csv')

    def run(self):
        with self.output().open('w') as f:
            print('May, 123', file=f)
            print('June, 560', file=f)


class CreateDatabase(sqla.CopyToTable):
    columns = [
        (["month", String(64)], {}),
        (["amount", Float], {})
    ]
    connection_string = "sqlite:///test.db"  # in memory SQLite database
    table = "sales"  # name of the table to store data
    column_separator = ','

    def rows(self):
        with self.input()
        for row in [("item1", "property1"), ("item2", "property2")]:
            yield row

    def requires(self):
        return [DownloadFranceSales(),
                DownloadGermanySales()]


# columns defines the table schema, with each element corresponding
# to a column in the format (args, kwargs) which will be sent to
# the sqlalchemy.Column(*args, **kwargs)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    luigi.run(['CreateDatabase', '--local-scheduler'])
