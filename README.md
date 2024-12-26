Доброго времени суток всем, кто читатет описание к данному проекту.
Прикладываю ТЗ по которому я выполнял данный проект:

В файлах формата csv (количество файлов заранее неизвестно) построчно хранится информация:  
ID пользователя (уникальный для каждого пользователя)  
Табельный номер пользователя (уникальный для каждого пользователя)  
Город пользователя  
Должность пользователя  
Дата рабочего дня  
Количество обработанных заявок  

Количество отработанных часов
Разделитель данных в csv – точка с запятой.
В папку изначально загружено какое-то количество файлов. Далее файлы могут добавляться еще.
Что требуется:
1. Загрузить данные из всех файлов в таблицы, разделив на 2 таблицы:  
1.1. Пользователи (ID пользователя, Табельный номер пользователя, Город пользователя, Должность пользователя)  
1.2. Информация о работе (Дата рабочего дня, Количество отработанных заявок, количество отработанных часов)  
Нужно учесть, что одной записи в таблице «Пользователи» может соответствовать несколько записей в таблице «Информация о работе», то есть они должны быть связаны по какому-то реквизиты. Пользователи в таблице «Пользователи» должны быть неповторяющиеся.
Способ реализации таблиц – списки, словари, массив, dataframe – на решение разработчика. Важно, что данные из таблиц должны быть доступны при следующем запуске приложения даже в том случае, если исходные csv-файлы уже удалены.
По результату загрузки должна быть выведена информация о количество загруженных файлов.
2. У пользователя должна быть возможность положить в папку новую файлы и запустить операцию добавления данных из этих файлов в созданные в пункте 1 таблицы.  
3. У пользователя должна быть возможность общения с приложением через строку ввода команд, где пользователю предлагается ввести код команды, которую надо выполнить:  
3.1. Команда SHOW_INFO – на экран выводится информация о количестве пользователей и количестве записей в таблице «Информация о работе».  
3.2. Команда SHOW_USER_<id пользователя> - на экран выводится вся информация о пользователе с указанным <id пользователя> из таблицы «Пользователи» в читаемом виде!  
3.3. Команда SHOW_JOB_<id пользователя> - на экран выводится информация о работе пользователя с указанным <id пользователя> в следующем формате: общее количество отработанных дней, среднее количество отработанных часов в день, общее количество обработанных заявок, среднее количество обработанных заявок и количество обработанных заявок за последний день.  
3.4. Команда LOAD_NEW – запуск загрузки новых файлов csv. По результату загрузки должна быть выведена информация о количество загруженных файлов.

ВНИМАНИЕ!!!
Для разворачивания проекта у себя на машине, создайте в папке проекта каталоги:  
archive_files  
database  
В папке new_files уже лежат csv-файлы для демонстрации работы  
Ставьте лайки, давайте обратную связь  
tg: @setdefaultrep
****