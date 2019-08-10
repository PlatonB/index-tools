__version__ = 'V2.2'

print('''
Конструктор запросов к большим таблицам.

Автор: Платон Быкадоров (platon.work@gmail.com), 2019.
Версия: V2.2.
Лицензия: GNU General Public License version 3.
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/index-tools/blob/master/README.md

Обязательно!
Перед запуском программы нужно установить модуль:
sudo pip3 install mysql-connector-python

Таблицы, по которым будет производиться поиск,
должны соответствовать таким требованиям:
1. Если их несколько, то одинаковой структуры;
2. Содержать шапку (одну и ту же для всех);
3. Каждая по отдельности - сжата в GZIP/BGZIP.
Пример подходящих данных - *vcf.gz-файлы проекта 1000 Genomes (придётся исключить
файл по Y-хромосоме, т.к. он с меньшим количеством столбцов, чем у остальных):
ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/supporting/GRCh38_positions/

Запросы на диапазон значений числового столбца (between)
выполняются быстро, только если этот столбец отсортирован.

Если настройки, запрашиваемые в рамках интерактивного
диалога, вам непонятны - пишите, пожалуйста, в Issues.
''')

print('\nИмпорт модулей программы...')

import sys

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

import mysql.connector, os, gzip
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

#Стандартные действия для подключения к БД.
cnx = mysql.connector.connect(user=user)
cursor = cnx.cursor()
cursor.execute(f'USE {db_name}')

cont, where = 'y', []
while cont not in ['no', 'n', '']:
        
        if len(col_names_n_types) > 1:
                col_name = input('\nИмя столбца, в котором ищем: ')
                if col_name not in col_names_n_types:
                        print(f'{col_name} - недопустимая опция')
                        sys.exit()
                        
        else:
                col_name = list(col_names_n_types.keys())[0]
                
        if col_names_n_types[col_name] == 'char':
                operator = input('''\nЛогический оператор
(игнорирование ввода ==> поиск будет по самим словам)
[not in|in(|<enter>)]: ''').upper()
                if operator not in ['NOT IN', 'IN', '']:
                        print(f'{operator} - недопустимая опция')
                        sys.exit()
                        
        else:
                operator = input('''\nЛогический оператор
(игнорирование ввода ==> поиск будет по самим числам)
(between - поиск от числа 1 до числа 2 включительно)
[>|<|>=|<=|between|not in|in(|<enter>)]: ''').upper()
                if operator not in ['>', '<', '>=', '<=', 'BETWEEN', 'NOT IN', 'IN', '']:
                        print(f'{operator} - недопустимая опция')
                        sys.exit()
                        
                elif operator in ['>', '<', '>=', '<=']:
                        cond = input(f'''\nПоисковое условие:
{operator} ''')
                        
                elif operator == 'BETWEEN':
                        cond = input('\nНижняя граница: ') + ' AND '
                        cond += input('\nВерхняя граница: ')
                where.append(f'({col_name} {operator} {cond})')
                
        if operator in ['NOT IN', 'IN', '']:
                if operator == '':
                        operator = 'IN'
                raw_cond = input(f'''\nПоисковое слово или несколько слов
(через запятую с пробелом):
{operator} ''').split(', ')
                cond = [f'"{word}"' for word in raw_cond]
                where.append(f'({col_name} {operator} ({", ".join(cond)}))')
                
        if len(col_names_n_types) > 1:
                cont = input('''\nИскать ещё в одном столбце?
(игнорирование ввода ==> нет)
[yes(|y)|no(|n|<enter>)]: ''')
                if cont not in ['yes', 'y', 'no', 'n', '']:
                        print(f'{cont} - недопустимая опция')
                        sys.exit()
                        
        else:
                break
        
#Поиск производится по всем таблицам базы.
#Т.е. даже, если в одной из них уже
#обнаружились соответствия запросу, обход
#будет продолжаться и завершится лишь
#после обращения к последней таблице.
for tab_name in tab_names:
        
        #Имена таблиц базы даных происходит от имён
        #соответствующих индексированных сжатых таблиц.
        #При формировании последних приходилось заменять
        #точки и дефисы на определённые кодовые слова.
        #Теперь для решения обратной задачи - получения
        #имени архива по имени таблицы - возвращаем точки
        #и дефисы на позиции альтернативных обозначений.
        arc_file_name = tab_name.replace('DOT', '.').replace('DEFIS', '-')
        
        #Конструируем имя конечного файла
        #и абсолютный путь к этому файлу.
        trg_file_name = f'found_in_{".".join(arc_file_name.split(".")[:-1])}'
        trg_file_path = os.path.join(trg_dir_path, trg_file_name)
        
        #Создаём флаг, по которому далее будет
        #определено, оказались ли в конечном
        #файле строки, отличные от хэдеров.
        empty_res = True
        
        #Открытие проиндексированного архива на чтение
        #и файла для поисковых результатов на запись.
        with gzip.open(os.path.join(arc_dir_path, arc_file_name)) as arc_file_opened:
                with open(trg_file_path, 'w') as trg_file_opened:
                        
                        #Конкатенируем элементы запроса, и
                        #прописываем полученную строку в конечный
                        #файл в качестве первого из хэдеров.
                        trg_file_opened.write(f'##{" AND ".join(where)}\n')
                        
                        #Созданная бэкендом база включает в
                        #себя также набор элементов шапки.
                        #Исходная последовательность этих
                        #элементов сохранена, поэтому из
                        #них легко собраем шапку обратно.
                        cursor.execute('SELECT header_cells FROM header')
                        header_line = '\t'.join([tup[0] for tup in cursor])
                        
                        #Прописываем восстановленную
                        #шапку в конечный файл.
                        #Это будет второй хэдер.
                        trg_file_opened.write(header_line + '\n')
                        
                        print(f'\nПоиск по таблице {tab_name} базы данных')
                        
                        #Инструкция, собственно, поиска.
                        #Собираем для неё запрос из списка
                        #сформированных ранее условий.
                        #Инструкция позволит извлечь из
                        #текущей таблицы БД байтовые позиции
                        #начала отвечающих этому запросу
                        #строк архивированной таблицы.
                        cursor.execute(f'''SELECT line_start FROM {tab_name}
                                           WHERE {" AND ".join(where)}''')
                        
                        print(f'Извлечение найденных строк таблицы {arc_file_name}')
                        
                        #Перемещение курсора по сжатой таблице к
                        #началу каждой отвечающей запросу строки.
                        #Очередная новая позиция курсора отсчитывается
                        #не от начала файла, а от последней запомненной
                        #позиции, что в ряде случаев приводит к
                        #достижению колоссальной производительности.
                        #Прописывание найденных строк в конечный файл.
                        #Присвоение флагу значения, показывающего
                        #наличие в конечном файле нехэдерных строк.
                        cur_pointer = 0
                        for line_start in cursor:
                                new_pointer = line_start[0]
                                arc_file_opened.seek(new_pointer - cur_pointer, 1)
                                trg_file_opened.write(arc_file_opened.readline().decode('UTF-8'))
                                cur_pointer = arc_file_opened.tell()
                                empty_res = False
                                
        #Если флаг-индикатор так и остался равен True,
        #значит, результатов поиска по данной таблице
        #нет, и в конечный файл попали только хэдеры.
        #Такие конечные файлы программа удалит.
        if empty_res == True:
                os.remove(trg_file_path)
                
cnx.close()
