import luigi


class SayHello(luigi.Task):
    def output(self):
        return luigi.LocalTarget('result.csv')

    def run(self):
        print('hello world')
        with self.output().open('w') as f:
            f.write('ok')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    luigi.run(['SayHello', '--local-scheduler'])
