__version__ = 'V1.0'

print('''
Программа пересечения и вычитания.

Автор: Платон Быкадоров (platon.work@gmail.com), 2019.
Версия: V1.0.
Лицензия: GNU General Public License version 3.
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/index-tools/blob/master/README.md

Обязательно!
Перед запуском программы нужно установить модуль:
sudo pip3 install mysql-connector-python

Таблицы, по одному из столбцов которых
предполагается пересекать или вычитать,
должны соответствовать таким требованиям:
1. Если их несколько, то одинаковой структуры;
2. Содержать шапку (одну и ту же для всех);
3. Каждая по отдельности - сжата в GZIP/BGZIP.
Пример подходящих данных - *vcf.gz-файлы проекта 1000 Genomes (придётся исключить
файл по Y-хромосоме, т.к. он с меньшим количеством столбцов, чем у остальных):
ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/supporting/GRCh38_positions/

Пересечение:
Столбец *каждой* левой таблицы пересекается
с тем же столбцом *всех* правых таблиц.

Вычитание:
Из столбца *каждой* левой таблицы вычетается
тот же столбец *всех* правых таблиц.

Если настройки, запрашиваемые в рамках интерактивного
диалога, вам непонятны - пишите, пожалуйста, в Issues.
''')

print('\nИмпорт модулей программы...')

import sys

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

import mysql.connector, copy, os, gzip
from backend.table_indexer import create_database

#Индексация выбранных пользователем
#столбцов исходных сжатых таблиц.
#При этом образуется БД, включающая
#в себя содержимое этих столбцов и
#индексы, представляющие собой байтовые
#позиции начала всех табличных строк.
#Получение путей к папке с упомянутыми
#таблицами и конечной папки, а также
#различных характеристик базы данных.
arc_dir_path, trg_dir_path, user, db_name, tab_names, col_names_n_types = create_database()

if len(tab_names) == 1:
        print('Для пересечения или вычитания требуется не менее двух таблиц')
        sys.exit()
        
#Стандартные действия для подключения к БД.
cnx = mysql.connector.connect(user=user)
cursor = cnx.cursor()
cursor.execute(f'USE {db_name}')

left_tab_names = input(f'''\nИмя одной или имена нескольких "левых" таблиц БД
(через запятую с пробелом)
(игнорирование ввода ==> все таблицы БД сделать "левыми")
[...|<enter>]: ''').split(', ')
if left_tab_names == ['']:
        left_tab_names = copy.deepcopy(tab_names)
        
right_tab_names = input(f'''\nИмя одной или имена нескольких "правых" таблиц БД
(через запятую с пробелом)
(игнорирование ввода ==> все таблицы БД сделать "правыми")
[...|<enter>]: ''').split(', ')
if right_tab_names == ['']:
        right_tab_names = copy.deepcopy(tab_names)
        
if len(col_names_n_types) > 1:
        col_name = input('\nИмя столбца, по которому пересекаем или вычитаем: ')
        if col_name not in col_names_n_types:
                print(f'{col_name} - недопустимая опция')
                sys.exit()
else:
        col_name = list(col_names_n_types.keys())[0]
        
action = input('''\nПересекать или вычитать табличные данные?
[intersect(|i)|subtract(|s)]: ''')
if action in ['intersect', 'i']:
        action, sign, right_tab_dest = 'int', '&', 'IS NOT NULL'
elif action in ['subtract', 's']:
        action, sign, right_tab_dest = 'sub', '-', 'IS NULL'
else:
        print(f'{action} - недопустимая опция')
        sys.exit()
        
#Созданная бэкендом БД также
#содержит отдельную таблицу с
#элементами пантабличной шапки.
#Исходная последовательность этих
#элементов сохранена, поэтому из
#них легко собраем шапку обратно.
cursor.execute('SELECT header_cells FROM header')
header_line = '\t'.join([tup[0] for tup in cursor])

#Все дальнейшие действия будут производиться
#для каждой левой таблицы по-отдельности.
for left_tab_name in left_tab_names:
        
        #Имена таблиц базы данных происходят от имён
        #соответствующих индексированных сжатых таблиц.
        #При формировании последних приходилось заменять
        #точки и дефисы на определённые кодовые слова.
        #Теперь для решения обратной задачи - получения
        #имени архива по имени таблицы - возвращаем точки
        #и дефисы на позиции альтернативных обозначений.
        left_arc_file_name = left_tab_name.replace('DOT', '.').replace('DEFIS', '-')
        right_arc_file_names = [right_tab_name.replace('DOT', '.').replace('DEFIS', '-') \
                                for right_tab_name in right_tab_names]
        
        #Конструируем имя конечного файла
        #и абсолютный путь к этому файлу.
        trg_file_name = f'{action}_res_{".".join(left_arc_file_name.split(".")[:-1])}'
        trg_file_path = os.path.join(trg_dir_path, trg_file_name)
        
        #Создаём флаг, по которому далее будет
        #определено, оказались ли в конечном
        #файле строки, отличные от хэдеров.
        empty_res = True
        
        #Открытие левого проиндексированного
        #архива на чтение и файла для
        #поисковых результатов на запись.
        with gzip.open(os.path.join(arc_dir_path, left_arc_file_name)) as left_arc_file_opened:
                with open(trg_file_path, 'w') as trg_file_opened:
                        
                        #Подготавливаем и прописываем первый хэдер.
                        #Им будет выражение, состоящее из названия
                        #текущей левой таблицы, знака, представляющего
                        #выбранное действие, и названий правых таблиц.
                        trg_file_opened.write(f'##{left_arc_file_name} {sign} {", ".join(right_arc_file_names)}\n')
                        
                        #Второй хэдер - ранее восстановленная
                        #из специальной таблицы БД шапка.
                        trg_file_opened.write(header_line + '\n')
                        
                        #И пересечение, и вычитание программа
                        #выполняет с помощью MySQL-алгоритма
                        #левостороннего внешнего объединения.
                        #Если не применить какие-либо фильтры,
                        #алгоритм выдаст все элементы столбца
                        #таблицы, имя которого вписано между
                        #FROM и первым LEFT JOIN инструкции.
                        #Если для элемента этого (левого)
                        #столбца не обнаружится такого же
                        #элемента одного из других (правых)
                        #столбцов, то взамен недостающего
                        #элемента выведется значение NULL.
                        left_join = [f'LEFT JOIN {right_tab_name} ON {left_tab_name}.{col_name} = {right_tab_name}.{col_name}' \
                                     for right_tab_name in right_tab_names if right_tab_name != left_tab_name]
                        
                        #Пересечение будет считаться успешным, если для
                        #данного элемента левого столбца ни в одном из
                        #правых столбцов не окажется соответствующего NULL.
                        #В результате же вычитания, от левого столбца останутся
                        #только отсутствующие во всех правых столбцах элементы.
                        where = [f'{right_tab_name}.{col_name} {right_tab_dest}' \
                                 for right_tab_name in right_tab_names if right_tab_name != left_tab_name]
                        
                        print(f'\nРабота с таблицей {left_tab_name} базы данных')
                        
                        #Инструкция, собственно, пересечения или вычитания.
                        #Она позволит извлечь из текущей левой таблицы БД
                        #байтовые позиции начала остающихся в результате выбранного
                        #действия строк соответствующей архивированной таблицы.
                        cursor.execute(f'''SELECT {left_tab_name}.line_start FROM {left_tab_name}
                                           {" ".join(left_join)}
                                           WHERE {" AND ".join(where)}
                                           ORDER BY {left_tab_name}.line_start''')
                        
                        print(f'Извлечение отобранных строк таблицы {left_arc_file_name}')
                        
                        #Перемещение курсора по сжатой таблице к
                        #началу каждой отвечающей запросу строки.
                        #При пересечении в правом столбце может найтись
                        #более одного соответствия элементу левого.
                        #Тогда, чтобы в конечную таблицу не попадали
                        #дубли, полученная из базы позиция новой
                        #строки сравнивается с позицией предыдущей.
                        #Если получилась нулевая дельта, то эта
                        #новая строка в конечный файл не пойдёт.
                        #Для достижения в некоторых случаях значительной
                        #оптимизиции производительности, очередная новая
                        #позиция курсора отсчитывается не от начала
                        #файла, а от последней запомненной позиции.
                        #Прописывание найденных строк в конечный файл.
                        #Присвоение флагу значения, показывающего
                        #наличие в конечном файле нехэдерных строк.
                        cur_pointer, new_pointer = 0, -1
                        for line_start in cursor:
                                if line_start[0] == new_pointer:
                                        continue
                                else:
                                        new_pointer = line_start[0]
                                left_arc_file_opened.seek(new_pointer - cur_pointer, 1)
                                trg_file_opened.write(left_arc_file_opened.readline().decode('UTF-8'))
                                cur_pointer = left_arc_file_opened.tell()
                                empty_res = False
                                
        #Если флаг-индикатор так и
        #остался равен True, значит,
        #результатов пересечения/вычитания
        #для данной левой таблицы нет, и в
        #конечный файл попали только хэдеры.
        #Такие конечные файлы программа удалит.
        if empty_res == True:
                os.remove(trg_file_path)
                
cnx.close()
