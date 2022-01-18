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
    Данные анализируются статистическим методом по правилу трех сигм.
    Если отклонение обнанужено, отправляется отчет в телеграмм.
    Функция ничего не возвращает.
    
    """
    chat_id = chat or -1001669395732
    
        
    # получаем из базы свежие данные плюс данные за 24 часа до этого из FEED_ACTIONS
    data_feed = Getch("""
            SELECT date_, users, date_24, users_24, views, views_24, likes, likes_24, CTR, CTR_24 
            FROM 
              (SELECT toStartOfFifteenMinutes(time) AS date_, uniq(user_id) AS users, countIf(post_id, action='view') AS views, 
                     countIf(post_id, action='like') AS likes, ROUND(likes*100/views, 2) AS CTR, toTime(date_) as time_ 
              FROM simulator.feed_actions 
              WHERE time BETWEEN now()-85000 AND now() 
              GROUP BY date_ ORDER BY date_) t1 
            JOIN 
              (SELECT toStartOfFifteenMinutes(time) AS date_24, uniq(user_id) AS users_24, countIf(post_id, action='view') AS views_24, 
                     countIf(post_id, action='like') AS likes_24, ROUND(likes_24*100/views_24, 2) AS CTR_24, toTime(date_24) as time_ 
              FROM simulator.feed_actions 
              WHERE time BETWEEN now()-171400 AND now()-86400 
              GROUP BY date_24 ORDER BY date_24) t2 
            USING time_ 
            ORDER BY date_
            """).df
    
    data_feed.drop(data_feed.shape[0]-1, inplace=True) # удаляем еще не заполненну пятнадцатиминутку
    
    # получаем из базы свежие данные плюс данные за 24 часа до этого из MESSAGE_ACTIONS
    data_message = Getch("""
            SELECT date_, users, users_24, date_24, mes, mes_24 
            FROM 
              (SELECT toStartOfFifteenMinutes(time) AS date_, uniq(user_id) AS users, COUNT(user_id) AS mes, toTime(date_) as time_ 
              FROM simulator.message_actions 
              WHERE time BETWEEN now()-85000 AND now() 
              GROUP BY date_ ORDER BY date_) t1 
            JOIN 
              (SELECT toStartOfFifteenMinutes(time) AS date_24, uniq(user_id) AS users_24, COUNT(user_id) AS mes_24, toTime(date_24) as time_ 
              FROM simulator.message_actions 
              WHERE time BETWEEN now()-171400 AND now()-86400 
              GROUP BY date_24 ORDER BY date_24) t2 
            USING time_ 
            ORDER BY date_
            """).df
    
    
    data_message.drop(data_message.shape[0]-1, inplace=True) 
    
    # вычесляем интервалы по правилу трех сигм
    #data_all[['left']] = round(data_all[['users']].rolling(semp_size, min_periods=1).mean() - data_all[['users']].rolling(semp_size, min_periods=1).std(ddof=0) * 3, 2)
    #data_all[['right']] = round(data_all[['users']].rolling(semp_size, min_periods=1).mean() + data_all[['users']].rolling(semp_size, min_periods=1).std(ddof=0) * 3, 2)
    
    '''
    ============================================
    Алерт по уникам в таблице feed_actions
    ============================================
    '''
    semp_size = 10 # размер интервала
    
    # вычесляем интервалы по правилу межквартильного интервала
    data_uniq_feed = data_feed[['date_', 'users', 'date_24', 'users_24']].copy()
    
    iqr = (data_uniq_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.75, interpolation='linear')) -\
            (data_uniq_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.25, interpolation='linear'))
    data_uniq_feed[['left']] = round(data_uniq_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.25, interpolation='linear') - iqr * 1.5, 2)
    data_uniq_feed[['right']] = round(data_uniq_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.75, interpolation='linear') + iqr * 1.5, 2)
    
    anom1 = get_anom(data_uniq_feed, semp_size)
    
    # проверяем последнее значение на предмет отклонения
    if anom1[0] == False:
        url_chart = 'http://superset\.lab\.karpov\.courses/r/165'
        url_DB = 'https://superset\.lab\.karpov\.courses/superset/dashboard/85/'
        metric_name = 'Уники по ленте'
        group = str(data_uniq_feed['date_'].iloc[-1]).replace('-', '\-')
        current_x = str(data_uniq_feed['users'].iloc[-1]).replace('.', '\.')
        x = str(anom1[1]).replace('.', '\.')
        full_name = 'Уникальные пользователи ленты'
        file_name = 'alarm_1.png'
        
        send_tg(data_uniq_feed, chat_id, url_chart, url_DB, metric_name, group, current_x, x, full_name, file_name)

    '''
    ============================================
    Алерт по просмотрам постов в таблице feed_actions
    ============================================
    '''
    semp_size = 10 # размер интервала
    
    # вычесляем интервалы по правилу межквартильного интервала
    data_view_feed = data_feed[['date_', 'views', 'date_24', 'views_24']].copy().rename(columns={'views':'users', 'views_24': 'users_24'})
    
    iqr = (data_view_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.75, interpolation='linear')) -\
            (data_view_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.25, interpolation='linear'))
    data_view_feed[['left']] = round(data_view_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.25, interpolation='linear') - iqr * 1.5, 2)
    data_view_feed[['right']] = round(data_view_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.75, interpolation='linear') + iqr * 1.5, 2)
    
    anom2 = get_anom(data_view_feed, semp_size)
    
    # проверяем последнее значение на предмет отклонения
    if anom2[0] == False:
        url_chart = 'http://superset\.lab\.karpov\.courses/r/170'
        url_DB = 'https://superset\.lab\.karpov\.courses/superset/dashboard/85/'
        metric_name = 'Просмотры постов'
        group = str(data_view_feed['date_'].iloc[-1]).replace('-', '\-')
        current_x = str(data_view_feed['users'].iloc[-1]).replace('.', '\.')
        x = str(anom2[1]).replace('.', '\.')
        full_name = 'Просмотры постов в ленте'
        file_name = 'alarm_2.png'

        send_tg(data_view_feed, chat_id, url_chart, url_DB, metric_name, group, current_x, x, full_name, file_name)

    '''
    ============================================
    Алерт по лайкам постов в таблице feed_actions
    ============================================
    '''
    semp_size = 10 # размер интервала
    
    # вычесляем интервалы по правилу межквартильного интервала
    data_like_feed = data_feed[['date_', 'likes', 'date_24', 'likes_24']].copy().rename(columns={'likes':'users', 'likes_24': 'users_24'})
    
    iqr = (data_like_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.75, interpolation='linear')) -\
            (data_like_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.25, interpolation='linear'))
    data_like_feed[['left']] = round(data_like_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.25, interpolation='linear') - iqr * 1.5, 2)
    data_like_feed[['right']] = round(data_like_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.75, interpolation='linear') + iqr * 1.5, 2)
    
    anom3 = get_anom(data_like_feed, semp_size)
    
    # проверяем последнее значение на предмет отклонения
    if anom3[0] == False:
        url_chart = 'http://superset\.lab\.karpov\.courses/r/170'
        url_DB = 'https://superset\.lab\.karpov\.courses/superset/dashboard/85/'
        metric_name = 'Лайки постов в ленте'
        group = str(data_like_feed['date_'].iloc[-1]).replace('-', '\-')
        current_x = str(data_like_feed['users'].iloc[-1]).replace('.', '\.')
        x = str(anom3[1]).replace('.', '\.')
        full_name = 'Лайки постов в ленте'
        file_name = 'alarm_3.png'
        
        send_tg(data_like_feed, chat_id, url_chart, url_DB, metric_name, group, current_x, x, full_name, file_name)

    '''
    ============================================
    Алерт по CTR лайков в таблице feed_actions
    ============================================
    '''
    semp_size = 16 # размер интервала
    
    # вычесляем интервалы по правилу межквартильного интервала
    data_ctr_feed = data_feed[['date_', 'CTR', 'date_24', 'CTR_24']].copy().rename(columns={'CTR':'users', 'CTR_24': 'users_24'})
    
    iqr = (data_ctr_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.75, interpolation='linear')) -\
            (data_ctr_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.25, interpolation='linear'))
    data_ctr_feed[['left']] = round(data_ctr_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.25, interpolation='linear') - iqr * 2, 2)
    data_ctr_feed[['right']] = round(data_ctr_feed[['users']].rolling(semp_size, min_periods=1).quantile(0.75, interpolation='linear') + iqr * 2, 2)
    
    anom4 = get_anom(data_ctr_feed, semp_size)
    
    # проверяем последнее значение на предмет отклонения
    if anom4[0] == False:
        url_chart = 'http://superset\.lab\.karpov\.courses/r/172'
        url_DB = 'https://superset\.lab\.karpov\.courses/superset/dashboard/85/'
        metric_name = 'CTR'
        group = str(data_ctr_feed['date_'].iloc[-1]).replace('-', '\-')
        current_x = str(data_ctr_feed['users'].iloc[-1]).replace('.', '\.')
        x = str(anom4[1]).replace('.', '\.')
        full_name = 'CTR'
        file_name = 'alarm_4.png'
        
        send_tg(data_ctr_feed, chat_id, url_chart, url_DB, metric_name, group, current_x, x, full_name, file_name)

    '''
    ============================================
    Алерт по уникам в таблице message_actions
    ============================================
    '''
    semp_size = 16 # размер интервала
    
    # вычесляем интервалы по правилу межквартильного интервала
    data_uniq_mes = data_message[['date_', 'users', 'date_24', 'users_24']].copy()
    
    iqr = (data_uniq_mes[['users']].rolling(semp_size, min_periods=1).quantile(0.75, interpolation='linear')) -\
            (data_uniq_mes[['users']].rolling(semp_size, min_periods=1).quantile(0.25, interpolation='linear'))
    data_uniq_mes[['left']] = round(data_uniq_mes[['users']].rolling(semp_size, min_periods=1).quantile(0.25, interpolation='linear') - iqr * 2, 2)
    data_uniq_mes[['right']] = round(data_uniq_mes[['users']].rolling(semp_size, min_periods=1).quantile(0.75, interpolation='linear') + iqr * 2, 2)
    
    anom5 = get_anom(data_uniq_mes, semp_size)
    
    # проверяем последнее значение на предмет отклонения
    if anom5[0] == False:
        url_chart = 'http://superset\.lab\.karpov\.courses/r/168'
        url_DB = 'https://superset\.lab\.karpov\.courses/superset/dashboard/85/'
        metric_name = 'Уники по меседжеру'
        group = str(data_uniq_mes['date_'].iloc[-1]).replace('-', '\-')
        current_x = str(data_uniq_mes['users'].iloc[-1]).replace('.', '\.')
        x = str(anom5[1]).replace('.', '\.')
        full_name = 'Уникальные пользователи меседжера'
        file_name = 'alarm_5.png'
        
        send_tg(data_uniq_mes, chat_id, url_chart, url_DB, metric_name, group, current_x, x, full_name, file_name)


    '''
    ============================================
    Алерт по количеству сообщений в таблице message_actions
    ============================================
    '''
    semp_size = 16 # размер интервала
    
    # вычесляем интервалы по правилу межквартильного интервала
    data_mes_send = data_message[['date_', 'users', 'date_24', 'users_24']].copy().rename(columns={'mes':'users', 'mes_24': 'users_24'})
    
    iqr = (data_mes_send[['users']].rolling(semp_size, min_periods=1).quantile(0.75, interpolation='linear')) -\
            (data_mes_send[['users']].rolling(semp_size, min_periods=1).quantile(0.25, interpolation='linear'))
    data_mes_send[['left']] = round(data_mes_send[['users']].rolling(semp_size, min_periods=1).quantile(0.25, interpolation='linear') - iqr * 2, 2)
    data_mes_send[['right']] = round(data_mes_send[['users']].rolling(semp_size, min_periods=1).quantile(0.75, interpolation='linear') + iqr * 2, 2)
    
    anom6 = get_anom(data_mes_send, semp_size)
    
    # проверяем последнее значение на предмет отклонения
    if anom6[0] == False:
        url_chart = 'http://superset\.lab\.karpov\.courses/r/169'
        url_DB = 'https://superset\.lab\.karpov\.courses/superset/dashboard/85/'
        metric_name = 'Отправлено сообщений'
        group = str(data_mes_send['date_'].iloc[-1]).replace('-', '\-')
        current_x = str(data_mes_send['users'].iloc[-1]).replace('.', '\.')
        x = str(anom6[1]).replace('.', '\.')
        full_name = 'Отправлено сообщений'
        file_name = 'alarm_6.png'
        
        send_tg(data_mes_send, chat_id, url_chart, url_DB, metric_name, group, current_x, x, full_name, file_name)

        
def get_anom(data_all, semp_size):
    """
    Функция получает на входе размер интервала и сам датафрем, проверяет, 
    выходит ли текущее значение метрики за границы интервала.
    Возвращает True/False если есть отклонение и саму величину 
    отклонения в % от стреднего значения за интервал.
    """
    anom = data_all['left'].values[-2] < data_all['users'].values[-1] < data_all['right'].values[-2]
    mean = round(np.mean(data_all['users'].values[-semp_size - 1: -1]), 2)
    dev = round(abs(data_all['users'].values[-1] - mean) * 100 / mean)
    return anom, dev

# функция для отправки сообщений по алерту
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
    
    error_mes = "__ALARM\!\!\!__ @igorUlymov\n"\
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
    g = sns.lineplot(data=data_all, x='date_', y='users', label='Сегодня', linewidth=2.5)
    g = sns.lineplot(data=data_all, x='date_', y='users_24', label='Вчера', linewidth=1, color='g')
    plt.fill_between(data=data_all, x='date_', y1='left', y2='right', alpha=.2)
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

try:
    alert_report(CHAT_ID)

    
except Exception as e:
    print(e)
