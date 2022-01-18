import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from read_db.CH import Getch
import pandas as pd
from settings import TG_TOKEN, CHAT_ID

sns.set()

# функция отправка алерта
def alert_report(chat=None):
    """
    Функция получает на входе id чата в телеграм, формирует запросы к базе и анализирует данные на предмет отклонений.
    Данные анализируются статистическим методом по методу межквартильного размаха.
    Если отклонение обнанужено, отправляется отчет в телеграмм.
    Функция ничего не возвращает.
    
    """
    chat_id = chat or -1001669395732
        
    # получаем из базы СВЕЖИЕ данные из таблицы FEED_ACTIONS
    data_feed_today = Getch("""
        SELECT toStartOfFifteenMinutes(time) AS date_, uniq(user_id) AS users, countIf(post_id, action='view') AS views, 
                     countIf(post_id, action='like') AS likes, ROUND(likes*100/views, 2) AS ctr 
        FROM simulator.feed_actions 
        WHERE time BETWEEN now()-86400 AND now() 
        GROUP BY date_ ORDER BY date_
        """).df
    # получаем из базы данные за предыдущие 9 дней из таблицы FEED_ACTIONS
    data_feed_old = Getch("""
        SELECT toStartOfFifteenMinutes(time) AS date_old, uniq(user_id) AS users_old, countIf(post_id, action='view') AS views_old, 
                     countIf(post_id, action='like') AS likes_old, ROUND(likes_old*100/views_old, 2) AS ctr_old 
        FROM simulator.feed_actions 
        WHERE time BETWEEN now()-864000 AND now()-86400 
        GROUP BY date_old ORDER BY date_old
        """).df
    
    data_feed_today.drop(data_feed_today.shape[0]-1, inplace=True) # удаляем еще не заполненну пятнадцатиминутку
    #data_feed_old.drop(data_feed_old.shape[0]-1, inplace=True) # удаляем еще не заполненну пятнадцатиминутку
    
    # выделяем время для последующего мёржа
    data_feed_today['time_'] = data_feed_today['date_'].dt.time
    data_feed_old['time_'] = data_feed_old['date_old'].dt.time
    
    # берем половину данных, чтобы текущие показания метрики были в середине графика, а не в конце,
    # а прогнозные показывали данные на пол дня вперед
    data_feed_today.iloc[0:48, 1:5] = None
    data_feed_today.iloc[0:48, 0] = data_feed_today.iloc[0:48, 0] + pd.Timedelta(days=1)
    '''
    ============================================
    Алерт по уникам в таблице feed_actions
    ============================================
    
    Комментарии к коду только для этого блока кода, 
    остальные алерты аналогичны по конструкции. 
    Отличаются только коэфициенты, т.к значения у всех метрик различные.
    
    '''
    # берем нужную часть ДФ    
    today_uniq = data_feed_today[['date_', 'users', 'time_']].copy()
    
    uniq_avg = data_feed_old[['users_old', 'time_']].groupby('time_', as_index=False).aggregate({'users_old':[per25, per50, per75]})
    
    # убрать мультииндекс
    uniq_avg.columns = ['time_', 'per25', 'per50', 'per75']
    
    # межквартильный размах
    uniq_avg['iqr'] = uniq_avg['per75'] - uniq_avg['per25']
    
    # левые и правые границы интервала. Коэфициент умножения iqr подобран эмпирически
    uniq_avg['left'] = round(uniq_avg['per25'] - 5 * uniq_avg['iqr'], 2)
    uniq_avg['right'] = round(uniq_avg['per75'] + 5 * uniq_avg['iqr'], 2)
    
    # сглаживаем границы, и делаем небольшой "подъем" всего интервала, т.к данные "растут" с течением времени.
    uniq_avg['left2'] = round(uniq_avg[['left']].rolling(4, 1).mean(), 2) + 40
    uniq_avg['right2'] = round(uniq_avg[['right']].rolling(4, 1).mean(), 2) + 40

    new_df_feed = today_uniq.merge(uniq_avg, on='time_').sort_values(by='date_').reset_index()
    
    last_ = new_df_feed[new_df_feed['users'].notna()][['date_', 'users', 'left2', 'right2']].iloc[-1:]
    
    # проверяем последнее показание на предмет отклонений
    anom1 = get_anom(last_)
    
    if anom1[0] == False: # формируем отчет, т.к метрика вышла за границы интервала
        url_chart = 'http://superset\.lab\.karpov\.courses/r/261'
        url_DB = 'https://superset\.lab\.karpov\.courses/superset/dashboard/85/'
        metric_name = 'Уники по ленте'
        group = str(last_['date_'].iloc[0]).replace('-', '\-')
        current_x = str(last_['users'].iloc[0]).replace('.', '\.')
        x = str(anom1[1]).replace('.', '\.')
        full_name = 'Уникальные пользователи ленты'
        file_name = 'alarm_1.png'
        
        # шлем алерт в телеграм
        send_tg(new_df_feed, chat_id, url_chart, url_DB, metric_name, group, current_x, x, full_name, file_name)

     
    '''
    ============================================
    Алерт по просмотрам постов в таблице feed_actions
    ============================================
    '''
        
    today_views = data_feed_today[['date_', 'views', 'time_']].copy()
    
    views_avg = data_feed_old[['views_old', 'time_']].groupby('time_', as_index=False).aggregate({'views_old':[per25, per50, per75]})
    
    views_avg.columns = ['time_', 'per25', 'per50', 'per75']
    views_avg['iqr'] = views_avg['per75'] - views_avg['per25']
    
    views_avg['left'] = round(views_avg['per25'] - 0.7 * views_avg['iqr'], 2) + 1200
    views_avg['right'] = round(views_avg['per75'] + 0.7 * views_avg['iqr'], 2) + 1200
    
    views_avg['left2'] = round(views_avg[['left']].rolling(2, 1).mean(), 2)
    views_avg['right2'] = round(views_avg[['right']].rolling(2, 1).mean(), 2)

    new_df_views = today_views.merge(views_avg, on='time_').sort_values(by='date_').reset_index().rename(columns={'views':'users'})
    
    last_2 = new_df_views[new_df_views['users'].notna()][['date_', 'users', 'left2', 'right2']].iloc[-1:]
    anom2 = get_anom(last_2)
    
    if anom2[0] == False:
        url_chart = 'http://superset\.lab\.karpov\.courses/r/260'
        url_DB = 'https://superset\.lab\.karpov\.courses/superset/dashboard/85/'
        metric_name = 'Просмотры постов'
        group = str(last_2['date_'].iloc[0]).replace('-', '\-')
        current_x = str(last_2['users'].iloc[0]).replace('.', '\.')
        x = str(anom2[1]).replace('.', '\.')
        full_name = 'Просмотры постов в ленте'
        file_name = 'alarm_2.png'

        send_tg(new_df_views, chat_id, url_chart, url_DB, metric_name, group, current_x, x, full_name, file_name)

    '''
    ============================================
    Алерт по лайкам постов в таблице feed_actions
    ============================================
    '''
        
    today_likes = data_feed_today[['date_', 'likes', 'time_']].copy()
    
    likes_avg = data_feed_old[['likes_old', 'time_']].groupby('time_', as_index=False).aggregate({'likes_old':[per25, per50, per75]})
    
    likes_avg.columns = ['time_', 'per25', 'per50', 'per75']
    likes_avg['iqr'] = likes_avg['per75'] - likes_avg['per25']
    
    likes_avg['left'] = round(likes_avg['per25'] - 0.5 * likes_avg['iqr'], 2) + 200
    likes_avg['right'] = round(likes_avg['per75'] + 0.5 * likes_avg['iqr'], 2) + 200
    
    likes_avg['left2'] = round(likes_avg[['left']].rolling(4, 1).mean(), 2)
    likes_avg['right2'] = round(likes_avg[['right']].rolling(4, 1).mean(), 2)

    new_df_likes = today_likes.merge(likes_avg, on='time_').sort_values(by='date_').reset_index().rename(columns={'likes':'users'})
    
    last_3 = new_df_likes[new_df_likes['users'].notna()][['date_', 'users', 'left2', 'right2']].iloc[-1:]
    anom3 = get_anom(last_3)
    
    if anom3[0] == False:
        url_chart = 'http://superset\.lab\.karpov\.courses/r/259'
        url_DB = 'https://superset\.lab\.karpov\.courses/superset/dashboard/85/'
        metric_name = 'Лайки постов в ленте'
        group = str(last_3['date_'].iloc[0]).replace('-', '\-')
        current_x = str(last_3['users'].iloc[0]).replace('.', '\.')
        x = str(anom3[1]).replace('.', '\.')
        full_name = 'Лайки постов в ленте'
        file_name = 'alarm_3.png'

        send_tg(new_df_likes, chat_id, url_chart, url_DB, metric_name, group, current_x, x, full_name, file_name)

      
    '''
    ============================================
    Алерт по CTR в таблице feed_actions
    ============================================
    '''
        
    today_ctr = data_feed_today[['date_', 'ctr', 'time_']].copy()
    ctr_avg = data_feed_old[['ctr_old', 'time_']].groupby('time_', as_index=False).aggregate({'ctr_old':[per25, per50, per75]})

    ctr_avg.columns = ['time_', 'per25', 'per50', 'per75']
    ctr_avg['iqr'] = ctr_avg['per75'] - ctr_avg['per25']

    ctr_avg['left'] = round(ctr_avg['per25'] - 1.5 * ctr_avg['iqr'], 2)
    ctr_avg['right'] = round(ctr_avg['per75'] + 1.5 * ctr_avg['iqr'], 2)

    ctr_avg['left2'] = round(ctr_avg[['left']].rolling(7, 1).mean(), 2) + 1
    ctr_avg['right2'] = round(ctr_avg[['right']].rolling(7, 1).mean(), 2) + 1

    new_df_ctr = today_ctr.merge(ctr_avg, on='time_').sort_values(by='date_').reset_index().rename(columns={'ctr':'users'})

    last_4 = new_df_ctr[new_df_ctr['users'].notna()][['date_', 'users', 'left2', 'right2']].iloc[-1:]
    anom4 = get_anom(last_4)
    
    if anom4[0] == False:
        url_chart = 'http://superset\.lab\.karpov\.courses/r/262'
        url_DB = 'https://superset\.lab\.karpov\.courses/superset/dashboard/85/'
        metric_name = 'CTR'
        group = str(last_4['date_'].iloc[0]).replace('-', '\-')
        current_x = str(last_4['users'].iloc[0]).replace('.', '\.')
        x = str(anom4[1]).replace('.', '\.')
        full_name = 'CTR'
        file_name = 'alarm_4.png'

        send_tg(new_df_ctr, chat_id, url_chart, url_DB, metric_name, group, current_x, x, full_name, file_name)

        
        
    # получаем из базы СВЕЖИЕ данные из таблицы MESSAGE_ACTIONS
    data_message_today = Getch("""
        SELECT toStartOfFifteenMinutes(time) AS date_, uniq(user_id) AS users, count(user_id) AS views 
        FROM simulator.message_actions 
        WHERE time BETWEEN now()-86400 AND now() 
        GROUP BY date_ ORDER BY date_
        """).df
    
    # получаем из базы данные за предыдущие 9 дней из таблицы MESSAGE_ACTIONS
    data_message_old = Getch("""
        SELECT toStartOfFifteenMinutes(time) AS date_old, uniq(user_id) AS users_old, count(user_id) AS views_old 
        FROM simulator.message_actions 
        WHERE time BETWEEN now()-864000 AND now()-86400 
        GROUP BY date_old ORDER BY date_old
        """).df
    
    data_message_today.drop(data_message_today.shape[0]-1, inplace=True) # удаляем еще не заполненну пятнадцатиминутку
    #data_message_old.drop(data_message_old.shape[0]-1, inplace=True) # удаляем еще не заполненну пятнадцатиминутку
    
    
    data_message_today['time_'] = data_message_today['date_'].dt.time
    data_message_old['time_'] = data_message_old['date_old'].dt.time
    
    data_message_today.iloc[0:48, 1:5] = None
    data_message_today.iloc[0:48, 0] = data_message_today.iloc[0:48, 0] + pd.Timedelta(days=1)        
    
    '''
    ============================================
    Алерт по уникам в таблице message_actions
    ============================================
    '''
        
    today_uniq_mes = data_message_today[['date_', 'users', 'time_']].copy()

    uniq_mes_avg = data_message_old[['users_old', 'time_']].groupby('time_', as_index=False).aggregate({'users_old':[per25, per50, per75]})

    uniq_mes_avg.columns = ['time_', 'per25', 'per50', 'per75']
    uniq_mes_avg['iqr'] = uniq_mes_avg['per75'] - uniq_mes_avg['per25']

    uniq_mes_avg['left'] = round(uniq_mes_avg['per25'] - 3.5 * uniq_mes_avg['iqr'], 2)
    uniq_mes_avg['right'] = round(uniq_mes_avg['per75'] + 3.5 * uniq_mes_avg['iqr'], 2)

    uniq_mes_avg['left2'] = round(uniq_mes_avg[['left']].rolling(3, 1).mean(), 2)
    uniq_mes_avg['right2'] = round(uniq_mes_avg[['right']].rolling(3, 1).mean(), 2)

    new_df_mes = today_uniq_mes.merge(uniq_mes_avg, on='time_').sort_values(by='date_').reset_index()

    last_5 = new_df_mes[new_df_mes['users'].notna()][['date_', 'users', 'left2', 'right2']].iloc[-1:]
    anom5 = get_anom(last_5)
    
    if anom5[0] == False:
        url_chart = 'http://superset\.lab\.karpov\.courses/r/263'
        url_DB = 'https://superset\.lab\.karpov\.courses/superset/dashboard/85/'
        metric_name = 'Уники по месенджеру'
        group = str(last_5['date_'].iloc[0]).replace('-', '\-')
        current_x = str(last_5['users'].iloc[0]).replace('.', '\.')
        x = str(anom5[1]).replace('.', '\.')
        full_name = 'Уникальные пользователи месенджера'
        file_name = 'alarm_5.png'

        send_tg(new_df_mes, chat_id, url_chart, url_DB, metric_name, group, current_x, x, full_name, file_name)
    
    '''
    ====================================================================
    Алерт по количеству отправленных сообщений в таблице message_actions
    ====================================================================
    '''
        
    today_count_mes = data_message_today[['date_', 'views', 'time_']].copy()

    mes_avg = data_message_old[['views_old', 'time_']].groupby('time_', as_index=False).aggregate({'views_old':[per25, per50, per75]})

    mes_avg.columns = ['time_', 'per25', 'per50', 'per75']
    mes_avg['iqr'] = mes_avg['per75'] - mes_avg['per25']

    mes_avg['left'] = round(mes_avg['per25'] - 3.5 * mes_avg['iqr'], 2)
    mes_avg['right'] = round(mes_avg['per75'] + 3.5 * mes_avg['iqr'], 2)

    mes_avg['left2'] = round(mes_avg[['left']].rolling(3, 1).mean(), 2)
    mes_avg['right2'] = round(mes_avg[['right']].rolling(3, 1).mean(), 2)

    new_mes_count = today_count_mes.merge(mes_avg, on='time_').sort_values(by='date_').reset_index().rename(columns={'views':'users'})

    last_6 = new_mes_count[new_mes_count['users'].notna()][['date_', 'users', 'left2', 'right2']].iloc[-1:]
    anom6 = get_anom(last_6)
    
    if anom6[0] == False:
        url_chart = 'http://superset\.lab\.karpov\.courses/r/264'
        url_DB = 'https://superset\.lab\.karpov\.courses/superset/dashboard/85/'
        metric_name = 'Отправлено сообщений'
        group = str(last_6['date_'].iloc[0]).replace('-', '\-')
        current_x = str(last_6['users'].iloc[0]).replace('.', '\.')
        x = str(anom6[1]).replace('.', '\.')
        full_name = 'Отправлено сообщений'
        file_name = 'alarm_6.png'

        send_tg(new_mes_count, chat_id, url_chart, url_DB, metric_name, group, current_x, x, full_name, file_name)


# функции для получения квартилей при агрегации
def per25(x):
    return x.quantile(.25)

def per75(x):
    return x.quantile(.75)

def per50(x):
    return x.quantile(.50)

# функция отправки оповещения
def send_tg(data_all, chat_id, url_chart, url_DB, metric_name, group, current_x, x, full_name, file_name):
    """
    Функция принимает на вход параметры необходимые для формирования отчета, 
        формирует текстовое и графическое оповещение в телеграм.
    Параметры:
        - data_all - датафрейм
        - chat_id - айди чата в телеге
        - url_chart - ссылка на диаграмму метрики
        - url_DB - ссылка на дашборд
        - metric_name - название метрики
        - group - временной срез когда срабатывает метрика
        - current_x - текущее значение метрики
        - x - отклонение от среднего значения по интервалу
        - full_name - название графика
        - file_name - название файла
    
    Функция ничего не возращает.
    """
    
    error_mes = "__ALARM\!\!\!__\n"\
        "Метрика *{metric_name}* в срезе *{group}*\.\n"\
        "Текущее значение\ *{current_x}*\. Отклонение более *{x}*\%\.\n"\
        "Чарт метрики\:\n {url_chart} \n"\
        "Дашборд:\n {url_DB}"
    
    bot = telegram.Bot(token=TG_TOKEN)

    bot.send_message(chat_id, text = error_mes.format(metric_name=metric_name, 
                                                      group=group, 
                                                      current_x=current_x, 
                                                      x=x,
                                                      url_chart=url_chart,
                                                      url_DB=url_DB), parse_mode=telegram.ParseMode.MARKDOWN_V2)

    plt.figure(figsize=(10, 11))
    plt.xticks(rotation=65, fontsize=14)
    plt.title(full_name, fontsize=25, pad=25)
    g = sns.lineplot(data=data_all, x='date_', y='users', linewidth=2, color='r')
    plt.fill_between(data=data_all, x='date_', y1='left2', y2='right2', alpha=.2)
    g.legend(fontsize=14, loc='upper left')
    plt.xlabel("Время, 15мин", fontdict={'fontsize': 18}, labelpad=10)
    plt.ylabel("Количество", fontdict={'fontsize': 18}, labelpad=10)
    ylabels = ['{:,.0f}'.format(i) for i in g.get_yticks()]
    g.set_yticklabels(ylabels, fontdict={'fontsize': 14})
    plt.tight_layout(pad=2, h_pad=2)
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = file_name
    plot_object.seek(0)
    plt.close()
    bot.sendPhoto(chat_id, photo=plot_object)

# функция проверки отклонения метрики и подсчета размера отклонения
def get_anom(data_):
    """
    Функция получает строку датафрема, и проверяем
    выходит ли текущее значение метрики за границы интервала.
    Возвращает True/False если есть отклонение и саму величину 
    отклонения в % от стреднего значения за интервал. 
    Среднее по интервалу считается по границам интервала.
    """
    anom = data_['left2'].values[0] < data_['users'].values[0] < data_['right2'].values[0]
    mean = np.mean([data_['left2'].values[0], data_['right2'].values[0]])
    dev = round(abs(data_['users'].values[0] - mean) * 100 / mean)
    return anom, dev

try:
    alert_report(CHAT_ID)
    
except Exception as e:
    print(e)
