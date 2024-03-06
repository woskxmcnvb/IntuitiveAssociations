import io
import os

import json

import pandas as pd 

import seaborn as sns
import seaborn.objects as so
import matplotlib.pyplot as plt

from sklearn.linear_model import LinearRegression

from ExcelReportBuilder import ExcelReportBuilder
    

class IADatabase: 
    df_ = None
    
    job_type_categories_ = ['ad.look', 'adhoq']
    ad_brand_categories_ = ['ad', 'brand']
    wtype_categories_ =    ['warm_up', 'active']
    ia_answers_categories_ =  ['Yes', 'No']
    
    time_from_ = 150
    time_to_ = 2500

    resp_speed_regression_ = None
    
    COLUMNS_ = ['JOB_ID', 'JOB_TYPE', 'QST_NO', 
                'IA_CELL', 'IA_ORD', 'IA_WORDS', 'IA_MS', 'IA_ANSWER', 'IA_ATTEMP', 'IA_AD_BRAND', 'IA_WTYPE']
    
    fast_slow_calculated_ = False
    
    
    def __init__(self, *args, **kwargs):
        self.df_ = pd.DataFrame({
            'JOB_ID':       pd.Series(dtype = 'int'),
            'JOB_TYPE':     pd.Categorical([], categories=self.job_type_categories_, ordered=False),
            'QST_NO':       pd.Series(dtype = 'int'),
            'IA_CELL':      pd.Series(dtype = 'int'),
            'IA_ORD':       pd.Series(dtype = 'int'),
            'IA_WORDS':     pd.Categorical([]),
            'IA_MS':        pd.Series(dtype = 'int'),
            'IA_ANSWER':    pd.Categorical([], categories=self.ia_answers_categories_, ordered=False),
            'IA_ATTEMP':    pd.Series(dtype = 'int'),
            'IA_AD_BRAND':  pd.Categorical([], categories=self.ad_brand_categories_, ordered=False),
            'IA_WTYPE':     pd.Categorical([], categories=self.wtype_categories_, ordered=False)
        })

    def __getattr__(self, attr):
        return getattr(self.df_, attr)
    
    def Deserialize(self, file_name): 
        self.df_ = pd.read_pickle(file_name)
        
    def Size(self): 
        return len(self.df_)
    
    def Serialize(self, file_name): 
        self.df_.to_pickle(file_name)
        
    def IsJobInDatabase(self, job_id): 
        if job_id in self.df_['JOB_ID'].unique(): 
            return True
        else: 
            return False
        
    def DropJobs(self, job_list): 
        self.df_ = self.df_[~self.df_['JOB_ID'].isin(job_list)]
        
    
    def AppendNewData(self, new_data):
        #################
        ## проверить что все столбцы на месте - нужно сделать 
        #################
        
        self.df_['IA_WORDS'] = self.df_['IA_WORDS'].cat.set_categories(
            set(new_data['IA_WORDS'].cat.categories) | set(self.df_['IA_WORDS'].cat.categories)
        )
        new_data['IA_WORDS'] = new_data['IA_WORDS'].cat.set_categories(
            set(new_data['IA_WORDS'].cat.categories) | set(self.df_['IA_WORDS'].cat.categories)
        )
        
        self.df_ = pd.concat([self.df_, new_data[self.COLUMNS_]])
        
    def CleanRecordsFilter(self): 
        return ( 
             self.df_['IA_ANSWER'].notna() & 
            (self.df_['IA_WTYPE'] == 'active') & 
            (self.df_['IA_MS'] > self.time_from_) & 
            (self.df_['IA_MS'] < self.time_to_)
        )
    
    def GetCleanDB(self):
        return self.df_.loc[self.CleanRecordsFilter(), :]        
    
    def RespondentSpeedNorm(self, job_type, ad_brand): 
        selection = ['JOB_ID', 'QST_NO', 'IA_MS']
        
        _filter = ( self.GetShitFilter() & 
                   (self.df_['JOB_TYPE'].isin(job_type)) & 
                   (self.df_['IA_AD_BRAND'].isin(ad_brand)))
            
        _by_respondent = self.df_.loc[_filter, selection].groupby(['JOB_ID', 'QST_NO']).mean()
        
        return _by_respondent.mean().item()
    
    def GetNorms(self) -> pd.Series: 
        grouper = ['JOB_TYPE', 'IA_AD_BRAND', 'IA_ANSWER']
        selection = grouper + ['IA_MS']
        
        return self.loc[self.CleanRecordsFilter(), selection].groupby(grouper, observed=False).agg(['mean', 'std'])['IA_MS']
    
    def JobList(self):
        return self.df_['JOB_ID'].unique()
    

    
    def __BuildRespondentSpeedRegression(self):
        # инициализирует функцию, которая считает ожидаемую среднюю скорость респондента в зависимости от количества заданий
        cdb = self.GetCleanDB()
        tasks_speed = pd.concat(
                [
                    cdb.groupby(['JOB_TYPE', 'JOB_ID', 'QST_NO'], observed=True)['IA_MS'].count().groupby(['JOB_TYPE', 'JOB_ID'], observed=True).median(), 
                    cdb.groupby(['JOB_TYPE', 'JOB_ID', 'QST_NO'], observed=True)['IA_MS'].mean().groupby(['JOB_TYPE', 'JOB_ID'], observed=True).mean()
                ],
                axis=1
            ).set_axis(['Tasks', 'Speed'], axis='columns').reset_index()
        self.resp_speed_regression_ = LinearRegression().fit(tasks_speed[['Tasks']].values, tasks_speed['Speed'].values)

    def ExpectedRespondentSpeed(self, num_of_tasks):
        # считает ожидаемую среднюю скорость респондента в зависимости от количества заданий
        if not self.resp_speed_regression_:
            self.__BuildRespondentSpeedRegression()
        return self.resp_speed_regression_.predict(num_of_tasks)
    
    def __CalculateFastSlow(self):
        if self.fast_slow_calculated_: 
            return
        cdb = self.GetCleanDB()
        # количество заданий на респондента в проекта
        tasks = pd.DataFrame(cdb.groupby(['JOB_ID', 'QST_NO'], observed=True)['IA_MS'].count().groupby(['JOB_ID'], observed=True).median())
        # ожидаемая средняя скорость для такого количества заданий
        expected_speed = pd.Series(self.ExpectedRespondentSpeed(tasks.values), index=tasks.index)
        # актуальная средняя скорость по респондентам
        actual_speed = cdb.groupby(['JOB_ID', 'QST_NO'], observed=True)['IA_MS'].mean()
        # коэффициент расторопности респондента
        resp_speed_koef = (actual_speed / expected_speed).reset_index().set_axis(['JOB_ID', 'QST_NO', 'resp_koef'], axis='columns')
        # нормы... 
        norms = self.GetNorms()
        
        # все сливаем 
        self.df_ = self.df_\
            .merge(resp_speed_koef, how='left', on=['JOB_ID', 'QST_NO'])\
            .merge(norms.reset_index(), how='left', on=['JOB_TYPE', 'IA_AD_BRAND','IA_ANSWER'])
        
        # быстро или медленно
        self.df_['is_fast'] = 'slow'
        fast_condition = self.df_['IA_MS'] < self.df_['mean'] * self.df_['resp_koef'] - 0.5 * self.df_['std'] * (self.df_['resp_koef'] ** 0.5)
        self.df_.loc[fast_condition, 'is_fast'] = 'fast'
        self.df_['no_shit'] = self.CleanRecordsFilter()
        
        self.df_.loc[~self.CleanRecordsFilter(), 'is_fast'] = ""
        
        self.fast_slow_calculated_ = True

    def CalculateJob(self, job_id): 
        self.__CalculateFastSlow()
        selected_columns = ['QST_NO', 'IA_WORDS', 'is_fast', 'no_shit']
        return self.df_.loc[self.df_['JOB_ID'] == job_id, selected_columns]
    
    def GetChartNorms(self):
        self.__CalculateFastSlow()

        self.df_['Yes %'] = (self.df_['IA_ANSWER'] == 'Yes')
        self.df_['Fast %'] = (self.df_['is_fast'] == 'fast')

        cdb = self.GetCleanDB()

        return pd.concat(
            [
                cdb.groupby(['JOB_TYPE', 'JOB_ID', 'IA_AD_BRAND', 'IA_CELL', 'IA_WORDS'], observed=True)['Yes %'].mean()\
                    .groupby(['JOB_TYPE', 'IA_AD_BRAND'], observed=True).mean(), 
                cdb[cdb['Yes %']].groupby(['JOB_TYPE', 'JOB_ID', 'IA_AD_BRAND', 'IA_CELL', 'IA_WORDS'], observed=True)['Fast %'].mean()\
                    .groupby(['JOB_TYPE', 'IA_AD_BRAND'], observed=True).mean()
            ], 
            axis=1
        )


    
    
    


class IAReporter: 
    database_ = None
    config_ = None

    def __init__(self) -> None:
        self.database_ = IADatabase()

        with open('config.json', 'r') as f: 
            self.config_ = json.load(f)
        
    def ReadDataFile(self, file_name, job_id=None, job_type=None):
        
        new_data = pd.read_excel(
            file_name, 
            dtype={
                'JOB_ID':       'int',
                'QST_NO':       'int',
                'IA_CELL':      'int', 
                'IA_ORD':       'int',
                'IA_WORDS':     'category',
                'IA_MS':        'int',
                'IA_ANSWER':    'object',
                'IA_ATTEMP':    'object',
                'IA_AD_BRAND':  'category',
                'IA_WTYPE':     'category'
            }
        )
        
        #################
        ## проверка дублей - встречаются 
        #################

        if 'ad' in new_data['IA_AD_BRAND'].unique(): 
            job_type = 'ad.look'
        else: 
            job_type = 'adhoq'

        new_data['JOB_TYPE'] = pd.Categorical(
            len(new_data) * [job_type], categories=self.database_.job_type_categories_, 
            ordered=False) 
        
        for col in self.database_.columns: 
            if col not in new_data.columns: 
                print("ОШИБКА ЧТЕНИЯ: Не хватает столбца " + col)
                return
        
        if new_data['JOB_ID'].unique().size != 1:
            print("ОШИБКА ЧТЕНИЯ: В столбце JOB_ID ожидается один номер проекта")
            return
        
        if new_data['JOB_ID'].unique()[0] < 10000 or new_data['JOB_ID'].unique()[0] > 30000:
            print("ОШИБКА ЧТЕНИЯ: неверный номер проекта в JOB_ID (<10 000 или >30 000)")
            return
        
        if not all(x in self.database_.ad_brand_categories_ for x in new_data['IA_AD_BRAND'].cat.categories): 
            print("ОШИБКА ЧТЕНИЯ: Неправильные категории в столбце IA_AD_BRAND")
            return
        new_data['IA_AD_BRAND'] = new_data['IA_AD_BRAND'].cat.set_categories(self.database_.ad_brand_categories_, ordered=False)
        
        if not all(x in self.database_.wtype_categories_ for x in new_data['IA_WTYPE'].cat.categories): 
            print("ОШИБКА ЧТЕНИЯ: Неправильные категории в столбце IA_WTYPE")
            return 
        
        if all([x in new_data['IA_ANSWER'].unique() for x in self.database_.ia_answers_categories_]): 
            pass
        elif all([x in new_data['IA_ANSWER'].unique() for x in [1, 2]]): 
            new_data['IA_ANSWER'] = new_data['IA_ANSWER'].map({1: 'Yes', 2: 'No'})
        else: 
            print("ОШИБКА ЧТЕНИЯ: Неправильные категории в столбце IA_ANSWER")
            return 
        new_data['IA_ANSWER'] = pd.Categorical(new_data['IA_ANSWER'], categories=self.database_.ia_answers_categories_, ordered=False)
        
        new_data['IA_WTYPE'] = new_data['IA_WTYPE'].cat.set_categories(self.database_.wtype_categories_, ordered=False)
        new_data.loc[new_data['IA_ATTEMP'].isna(), 'IA_ATTEMP'] = 99
        new_data['IA_ATTEMP'] = new_data['IA_ATTEMP'].astype('int')
        
        return new_data

        

    def BuildJobReport(self, file_name): 

        ad = self.ReadDataFile(file_name)
        if ad is None: 
            print("Прервано")
            return
        
        job_id = ad['JOB_ID'].unique()[0]
        job_type = ad['JOB_TYPE'].unique()[0]

        self.database_.Deserialize(self.config_['database_path'])
        
        if self.database_.IsJobInDatabase(job_id): 
            print("Этот проект уже в базе")
        else: 
            self.database_.AppendNewData(ad)
            print("Этого проекта нет в базе. Добавлено")
            self.database_.Serialize(self.config_['database_path'])
        
        # расчет fast / slow в базе
        ad = ad.merge(self.database_.CalculateJob(job_id), how='left', on=['QST_NO', 'IA_WORDS'])

        # reporting from here on
        # chart norms
        if job_type == 'ad.look':
            chart_norms = self.database_.GetChartNorms().loc[('ad.look', 'brand')]
            chart_norm_yes = chart_norms['Yes %']
            chart_norm_fast = chart_norms['Fast %']
        else: 
            chart_norm_yes, chart_norm_fast = None, None

        excel_builder = ExcelReportBuilder(
            os.path.dirname(file_name) + "\\" + str(job_id) + "_report.xlsx"
            )
        excel_builder.AddTable(ad, 'data', drop_index=True)
        
        ia_table = IAReporter.BuildIATable(ad[ad['no_shit']])
        excel_builder.AddTable(ia_table, 'key_charts')
        
        for cell in ia_table.index.get_level_values('IA_CELL').unique():
            excel_builder.AddImage(
                IAReporter.PlotIAChart(ia_table.loc[(cell, 'brand')], 'Cell {}'.format(cell), chart_norm_yes, chart_norm_fast), 
                'key_charts', 
                'I{}'.format(1 + cell * 5)
                )

        excel_builder.SaveToFile()

        print('Отчет готов')

   
    @staticmethod
    def BuildIATable(ad: pd.DataFrame): 
        wt = pd.concat([
                ad['IA_ANSWER'] == 'Yes', 
                ad['is_fast'] == 'fast',
                ad[['IA_CELL', 'IA_AD_BRAND', 'IA_WORDS']]
            ], axis=1).set_axis(['Yes %', 'Fast %', 'IA_CELL', 'IA_AD_BRAND', 'IA_WORDS'], axis='columns')

        return wt.groupby(['IA_CELL', 'IA_AD_BRAND', 'IA_WORDS'], observed=True)[['Yes %', 'Fast %']].mean()
    
    @staticmethod
    def PlotIAChart(ia_table: pd.DataFrame, title: str="", yes_norm=None, fast_norm=None):
        plt.rc('xtick', labelsize=10, color='gray')    # fontsize of the tick labels
        plt.rc('ytick', labelsize=10, color='gray')    # fontsize of the tick labels
        
        fast_yes = (ia_table['Yes %'] > yes_norm) & (ia_table['Fast %'] > fast_norm)
        stream = io.BytesIO()
        
        _, ax = plt.subplots(figsize=(12, 6))
        sns.scatterplot(ia_table, x='Yes %', y='Fast %', 
                        hue=fast_yes, hue_order=[True, False], palette=['#a0cc00', '#bababa'], 
                        size=fast_yes, size_order=[True, False], sizes=[100, 100],
                        ax=ax)
        
        for _, row in ia_table.iterrows():
            plt.text(row['Yes %'], row['Fast %'], row.name, fontsize=11, 
                     horizontalalignment = ('left' if row['Yes %'] < 0.8 else 'right'))
        
        
        if yes_norm:
            ax.axvline(yes_norm, color='#bababa', linewidth=2)
        if fast_norm:
            ax.axhline(fast_norm, color='#bababa', linewidth=2)
        
        ax.set_xlabel('Yes %')
        ax.set_ylabel('Fast %')
        ax.set_title(title, fontsize=14)

        ax.legend().set_visible(False)
        plt.savefig(stream)
        plt.close()
        return stream