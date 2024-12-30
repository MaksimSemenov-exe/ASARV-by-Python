import tkinter as tk
from tkinter import messagebox, scrolledtext
import pandas as pd
import os
import glob
import traceback

# Основное окно приложения
window = tk.Tk()
window.title("ASARV GUI")
window.geometry("900x700")

# Глобальные переменные
only_users = pd.DataFrame()
only_info_about_work = pd.DataFrame()
my_df = pd.DataFrame()

# Пути для файлов
path = r'C:\Users\Acer\PycharmProjects\csv_project_\ASARV-by-Python\new_files'
archive_path = r'C:\Users\Acer\PycharmProjects\csv_project_\ASARV-by-Python\archive_files'
data_path_full = r'C:\Users\Acer\PycharmProjects\csv_project_\ASARV-by-Python\database\my_df.pkl'
columns = ['user_id', 'user_num', 'user_post', 'user_city', 'date', 'completed_applications', 'work_hours']


# Функции приложения
def load_pickle():
    """Загрузка данных из Pickle."""
    global my_df
    try:
        if os.path.exists(data_path_full):
            my_df = pd.read_pickle(data_path_full)
            load_to_tables()
            messagebox.showinfo("Success", "Pickle файл загружен.")
        else:
            messagebox.showwarning("Warning", "Pickle файл не найден.")
    except Exception as e:
        messagebox.showerror("Error", f"Ошибка загрузки Pickle файла: {e}")
        print(traceback.format_exc())


def load_to_tables():
    """Разделение данных на таблицы."""
    global only_info_about_work, only_users, my_df
    only_users = my_df[['user_id', 'user_num', 'user_post', 'user_city']].drop_duplicates(subset=['user_id'])
    only_info_about_work = my_df[['user_id', 'date', 'completed_applications', 'work_hours']]


def show_info():
    """Отображение общей информации."""
    global only_users, only_info_about_work
    info = f"Количество пользователей: {len(only_users)}\nКоличество записей о работе: {len(only_info_about_work)}"
    messagebox.showinfo("Information", info)


def show_user():
    """Отображение информации о пользователе."""
    global only_users
    user_id = user_id_entry.get()
    if user_id:
        user_info = only_users[only_users['user_id'] == user_id]
        if not user_info.empty:
            result_text.delete("1.0", tk.END)
            # Форматируем вывод
            for index, row in user_info.iterrows():
                result_text.insert(tk.END, f"ID: {row['user_id']}\n")
                result_text.insert(tk.END, f"Личный номер: {row['user_num']}\n")
                result_text.insert(tk.END, f"Должность: {row['user_post']}\n")
                result_text.insert(tk.END, f"Город: {row['user_city']}\n")
                result_text.insert(tk.END, "-"*40 + "\n")
        else:
            messagebox.showinfo("Not Found", f"Пользователь с ID {user_id} не найден.")
    else:
        messagebox.showwarning("Warning", "Введите ID пользователя.")


def show_job():
    """Отображение информации о работе пользователя."""
    global only_info_about_work
    user_id = user_id_entry.get()
    if user_id:
        work_data = only_info_about_work[only_info_about_work['user_id'] == user_id]
        if not work_data.empty:
            total_days = len(work_data)
            total_hours = work_data['work_hours'].astype(int).sum()
            total_tasks = work_data['completed_applications'].astype(int).sum()
            result = (
                f"Общее количество отработанных дней: {total_days}\n"
                f"Общее количество часов: {total_hours}\n"
                f"Общее количество заявок: {total_tasks}\n"
            )
            result_text.delete("1.0", tk.END)
            result_text.insert(tk.END, result)
        else:
            messagebox.showinfo("Not Found", f"Рабочие данные для ID {user_id} не найдены.")
    else:
        messagebox.showwarning("Warning", "Введите ID пользователя.")


def load_new_files():
    """Проверка и загрузка новых файлов."""
    global my_df
    new_files = glob.glob(os.path.join(path, "*.csv"))
    if not new_files:
        return

    try:
        for filename in new_files:
            with open(filename, 'r', encoding='utf-8') as f:
                new_df = pd.read_csv(f, header=None, sep=';', dtype=str)
            if new_df.shape[1] != len(columns):
                raise ValueError(f"Файл {filename} имеет неверное количество колонок.")
            new_df.columns = columns
            my_df = pd.concat([my_df, new_df], ignore_index=True)

            # Перемещаем обработанный файл
            archive_file = os.path.join(archive_path, os.path.basename(filename))
            os.replace(filename, archive_file)

        my_df.to_pickle(data_path_full)
        load_to_tables()
        messagebox.showinfo("Success", "Новые файлы успешно загружены.")
    except Exception as e:
        messagebox.showerror("Error", f"Ошибка при обработке файлов: {e}")
        print(traceback.format_exc())


# Проверяем наличие новых файлов при запуске
load_pickle()
load_new_files()


# UI компоненты
frame_top = tk.Frame(window)
frame_top.pack(pady=10)

tk.Label(frame_top, text="ID пользователя:").pack(side=tk.LEFT, padx=5)
user_id_entry = tk.Entry(frame_top, width=20)
user_id_entry.pack(side=tk.LEFT, padx=5)

frame_buttons = tk.Frame(window)
frame_buttons.pack(pady=10)

tk.Button(frame_buttons, text="Общая информация", command=show_info).pack(side=tk.LEFT, padx=5)
tk.Button(frame_buttons, text="Данные пользователя", command=show_user).pack(side=tk.LEFT, padx=5)
tk.Button(frame_buttons, text="Данные о работе", command=show_job).pack(side=tk.LEFT, padx=5)

frame_results = tk.Frame(window)
frame_results.pack(pady=10, fill=tk.BOTH, expand=True)

result_text = scrolledtext.ScrolledText(frame_results, wrap=tk.WORD, height=25, width=100)
result_text.pack(fill=tk.BOTH, expand=True)

# Запуск приложения
window.mainloop()
