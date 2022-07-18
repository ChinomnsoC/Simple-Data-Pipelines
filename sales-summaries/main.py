import luigi
from luigi import Task, LocalTarget, WrapperTask


class DownloadSalesData(Task):
    def output(self):
        return LocalTarget('all_sales.csv')

    def run(self):
        with self.output().open('w') as f:
            print('France,May,140', file=f)
            print('Germany,June,160', file=f)
            print('France,January,430', file=f)
            print('Germany,May,910', file=f)
            print('France,June,190', file=f)
            print('Germany,January,135', file=f)
            print('France,May,315', file=f)
            print('Germany,June,652', file=f)
            print('France,January,100', file=f)


class GetFranceSales(Task):
    def requires(self):
        return DownloadSalesData()

    def output(self):
        return LocalTarget('france_sales.csv')

    def run(self):
        with self.output().open('w') as out:
            with self.input().open() as f:
                for line in f:
                    if line.startswith('France'):
                        out.write(line)


class SummarizeFranceSales(Task):
    def requires(self):
        return GetFranceSales()

    def output(self):
        return LocalTarget('summary_france_sales.csv')

    def run(self):
        total = 0
        with self.input().open() as f:
            for line in f:
                separate_sales = line.split(',')
                total += float(separate_sales[2])
        with self.output().open('w') as out:
            out.write(str(total))


class GetGermanySales(Task):
    def requires(self):
        return DownloadSalesData()

    def output(self):
        return LocalTarget('germany_sales.csv')

    def run(self):
        with self.output().open('w') as out:
            with self.input().open() as f:
                for line in f:
                    if line.startswith('Germany'):
                        out.write(line)


class SummarizeGermanySales(Task):
    def requires(self):
        return GetGermanySales()

    def output(self):
        return LocalTarget('summary_germany_sales.csv')

    def run(self):
        total = 0
        with self.input().open() as f:
            for line in f:
                separate_sales = line.split(',')
                total += float(separate_sales[2])
        with self.output().open('w') as out:
            out.write(str(total))


class Final(WrapperTask):
    def requires(self):
        return [SummarizeGermanySales(),
                SummarizeFranceSales()]


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    luigi.run(['Final', '--local-scheduler'])

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
