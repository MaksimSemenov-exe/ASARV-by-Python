import glob
import pandas as pd
import datetime
import os
import warnings
import traceback
from io import StringIO

warnings.filterwarnings('ignore')

# Путь к папке с CSV-файлами
path = r'/new_files'
archive_path = r'archive_files'
data_path = r'database'
data_path_full = r'database/my_df.pkl'
pkl_filename = 'my_df.pkl'

filenames = glob.glob(path + "/*.csv")

# Ожидаемые колонки
columns = ['user_id', 'user_num', 'user_post', 'user_city', 'date', 'completed_applications', 'work_hours']
my_df = pd.DataFrame()


def load_pickle():
    if os.listdir(data_path):
        print('Загружаю pickle')
        df = pd.read_pickle(r'database/my_df.pkl')
        return df
    else:
        print('Нет файла')


# Функция для чтения CSV-файлов
def read_csv(df):
    global my_df
    if not os.listdir(path):
        print('Нет файлов для загрузки')
    else:
        for filename in filenames:
            try:
                print(f"Обработка файла: {filename}")

                # Читаем файл, указываем разделитель и типы данных
                with open(filename, 'r', encoding='utf-8') as f:
                    time_df = pd.read_csv(f, header=None, sep=';', dtype=str)

                # Проверяем соответствие количества колонок
                if time_df.shape[1] != len(columns):
                    raise ValueError(f"Файл {filename} имеет {time_df.shape[1]} колонок, ожидается {len(columns)}.")
                # Присваиваем заголовки колонок
                time_df.columns = columns

                # Добавляем к общему DataFrame
                my_df = pd.concat([my_df, time_df], ignore_index=True)

                # Перемещаем обработанный файл
                print(f"Перемещение файла: {filename}")
                new_location = os.path.join(archive_path, os.path.basename(filename))
                os.replace(filename, new_location)
                print(f'Файл {filename} успешно перемещён в {new_location}.')

            except Exception as e:
                print(f"Ошибка при обработке файла {filename}: {e}")
                print(traceback.format_exc())

    my_df = pd.concat([my_df, df])

    csv_counter = len(filenames)
    print(f'В программу загружено {csv_counter} файлов')
    file_path = os.path.join('database', 'my_df.pkl')
    my_df.to_pickle(file_path)
    return file_path


# Функция для разделения данных на таблицы
def load_to_tables(df):
    global only_info_about_work, only_users, pkl_filename, my_df
    only_users = my_df[['user_id', 'user_num', 'user_post', 'user_city']].drop_duplicates(subset=['user_id'])
    # Информация о работе
    only_info_about_work = my_df[['user_id', 'date', 'completed_applications', 'work_hours']]
    return only_info_about_work, only_users


# Функция для сохранения данных в файлы
def get_csv():
    date_and_time = datetime.datetime.now().strftime('%d.%m.%Y %H:%M')
    # Сохраняем без индекса


#    only_users.to_csv('info about users.csv', encoding='utf-8', index=False)
#   only_info_about_work.to_csv('info about work.csv', encoding='utf-8', index=False)


get_csv()


# Функция для отображения информации
def show_info():
    global only_users, only_info_about_work
    print(f'Количество пользователей: {len(only_users)}')
    print(f'Количество записей о работе: {len(only_info_about_work)}')


# Функция для отображения информации о пользователе
def show_user():
    global only_users
    worker_id = str(input('Введите id работника, данные которого вы бы хотели просмотреть: '))

    information = only_users[only_users['user_id'] == worker_id]
    information['user_id'] = information['user_id'].astype(str)
    information['user_num'] = information['user_num'].astype(str)
    information['user_post'] = information['user_post'].astype(str)
    information['user_city'] = information['user_city'].astype(str)

    user_id = information['user_id']
    user_num = information['user_num']
    user_post = information['user_post']
    user_city = information['user_city']

    print(
        f'ID работника: {user_id} \n Личный номер работника: {user_num} \n Город работника: {user_city} \n Должность работника: {user_post}')


def check_for_new():
    global only_users
    global only_info_about_work
    if os.listdir(data_path):
        ...
    else:
        my_df = pd.read_pickle(data_path_full)
        only_users = my_df[['user_id', 'user_num', 'user_post', 'user_city']].drop_duplicates(subset=['user_id'])
        # Информация о работе
        only_info_about_work = my_df[['user_id', 'date', 'completed_applications', 'work_hours']]
        return only_users, only_info_about_work


def show_job(work_info):
    global only_info_about_work
    if 'only_info_about_work' not in globals():
        print("Переменная only_info_about_work не инициализирована.")
        return
    id_for_searching = str(input('Введите ID работника: '))
    print(f'Содержимое: {only_info_about_work}')
    frame_one_user = only_info_about_work[only_info_about_work['user_id'] == id_for_searching]
    frame_one_user.reset_index(drop=True, inplace=True)
    frame_one_user.fillna(0)
    frame_one_user['work_hours'] = frame_one_user['work_hours'].astype(int)
    frame_one_user['completed_applications'] = frame_one_user['completed_applications'].astype(int)

    work_hours_sum = frame_one_user['work_hours'].sum()
    work_hours_mean = frame_one_user['work_hours'].describe()['mean']

    work_tasks_sum = frame_one_user['completed_applications'].sum()
    work_tasks_mean = frame_one_user['completed_applications'].describe()['mean']

    frame_one_user['date'] = pd.to_datetime(frame_one_user['date'], format='mixed', dayfirst=True)
    last_date = frame_one_user['date'].max()

    last_day_tasks = frame_one_user[frame_one_user['date'] == last_date]
    last_day_tasks_value = last_day_tasks['completed_applications'].values[0]

    total_days = len(frame_one_user)

    print(
        f'Общее кол-во отработанных дней: {total_days} \n Среднее кол-во отработанных часов в день: {work_hours_mean} \n Общее кол-во обработанных заявок: {work_tasks_sum} \n Среднее кол-во обработанных заявок: {work_tasks_mean} \n Кол-во обработанных заявок за последний рабочий день: {last_day_tasks_value}')


def load_new():
    global my_df, only_info_about_work, only_users, df

    new_files = glob.glob(os.path.join(path, "*.csv"))

    if not new_files:
        print("Новых файлов не обнаружено.")
        return

    print(f"Обнаружено {len(new_files)} новых файлов.")

    for filename in new_files:
        try:
            print(f"Обработка файла: {filename}")

            with open(filename, 'r', encoding='utf-8') as f:
                new_df = pd.read_csv(f, header=None, sep=';', dtype=str, encoding='utf-8-sig', quoting=3)

            if new_df.shape[1] != len(columns):
                raise ValueError(f"Файл {filename} имеет {new_df.shape[1]} колонок, ожидается {len(columns)}.")

            new_df.columns = columns
            my_df = pd.concat([my_df, new_df], ignore_index=True)
            print(f"Файл {filename} успешно обработан.")

            archive_path = os.path.join('archive_files', os.path.basename(filename))
            os.rename(filename, archive_path)
            print(f"Файл {filename} перемещён в архив.")

        except Exception as e:
            print(f"Ошибка при обработке файла {filename}: {e}")
            print(traceback.format_exc())


    my_df = pd.concat([my_df, df])

    only_users = my_df[['user_id', 'user_num', 'user_post', 'user_city']].drop_duplicates(subset=['user_id'])
    only_info_about_work = my_df[['user_id', 'date', 'completed_applications', 'work_hours']]

    print(f"Количество уникальных пользователей: {len(only_users)}")
    print(f"Количество записей о работе: {len(only_info_about_work)}")


# Функция для выбора операции


df = load_pickle()
df = read_csv(df)
df = pd.read_csv(StringIO(df))
only_info_about_work, only_users = load_to_tables(df)

def question_about_operation():
    answer = str(input(
        'Вот все доступные операции: SHOW_INFO, SHOW_USER, SHOW_JOB, LOAD_NEW \\n Хотите воспользоваться одной из них? '))
    if answer == 'SHOW_INFO':
        show_info()
    elif answer == 'SHOW_USER':
        show_user()
    elif answer == 'SHOW_JOB':
        show_job(only_info_about_work)
    elif answer == 'LOAD_NEW':
        load_new()
    else:
        print('Неверная команда.')

while True:
    question_about_operation()
