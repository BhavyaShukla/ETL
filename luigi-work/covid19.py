import pymongo
import luigi
import pandas as pd

class CovidQ1(luigi.Task):

    def output(self):
        return luigi.LocalTarget("db1.csv")
    
    def run(self):
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["scs"]
        mycol = mydb["case_age"]
        mydoc = mycol.find({})
        df =  pd.DataFrame(list(mydoc))
        with self.output().open('w') as csv: 
            csv.write(df.to_csv())

class CovidQ2(luigi.Task):

    def output(self):
        return luigi.LocalTarget("db2.csv")
    
    def run(self):
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["scs"]
        mycol = mydb["case_country"]
        mydoc = mycol.find({})
        df =  pd.DataFrame(list(mydoc))
        with self.output().open('w') as csv: 
            csv.write(df.to_csv())

class CovidQ3(luigi.Task):

    def output(self):
        return luigi.LocalTarget("db3.csv")
    
    def run(self):
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["scs"]
        mycol = mydb["case_death"]
        mydoc = mycol.find({})
        df =  pd.DataFrame(list(mydoc))
        with self.output().open('w') as csv: 
            csv.write(df.to_csv())


class CovidQ4(luigi.Task):
    def requires(self):
        return [CovidQ1(),CovidQ2(),CovidQ3()]
    def output(self):
        return luigi.LocalTarget("Report.csv")
    def run(self):
        df1 = pd.read_csv("db1.csv", header = 0, encoding = 'utf-8',index_col = False)
        # column names for df2 are different I am not sure what to do there
        df2 = pd.read_csv("db2.csv", header = 0, encoding = 'utf-8',index_col = False)
        df3 = pd.read_csv("db3.csv", header = 0, encoding = 'utf-8',index_col = False)
        frame = [df1,df3]
        df4 = pd.concat(frame)
       # run command : python -m luigi --module covid19 CovidQ4 --local-scheduler
        with self.output().open('w') as csv: 
            csv.write(df4.to_csv())

    if __name__ == '__main__':
        luigi.run(main_task_cls=CovidQ4,local_scheduler=False)
       
