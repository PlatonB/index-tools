__version__ = 'V1.0'

print('''
Конструктор запросов к большим неотсортированным таблицам.

Автор: Платон Быкадоров (platon.work@gmail.com), 2019.
Версия: V1.0.
Лицензия: GNU General Public License version 3.
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/index-tools/blob/master/README.md

Обязательно! Установка модуля:
sudo pip3 install mysql-connector-python

Таблицы, в которых будет производиться поиск,
должны соответствовать таким требованиям:
1. Если их несколько, то одинаковой структуры;
2. Содержать шапку (одну и ту же для всех);
3. Каждая по отдельности - сжата в GZIP/BGZIP.
Пример подходящих данных - *vcf.gz-файлы проекта 1000 Genomes (придётся исключить
файл по Y-хромосоме, т.к. он с меньшим количеством столбцов, чем у остальных):
ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/supporting/GRCh38_positions/

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

#Получение путей к папке с индексируемыми
#сжатыми таблицами и конечной папки, а
#также различных характеристик базы данных.
#Сама БД включает в себя содержимое
#выбранных пользователем столбцов этих
#таблиц и индексы, представляющие
#собой байтовые позиции табличных строк.
arc_dir_path, trg_dir_path, user, db_name, tab_names, col_names_n_types = create_database()

#Стандартные действия для подключения к БД.
cnx = mysql.connector.connect(user=user)
cursor = cnx.cursor()
cursor.execute(f'USE {db_name}')

cont, query = 'y', []
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
                query.append(f'({col_name} {operator} {cond})')
                
        if operator in ['NOT IN', 'IN', '']:
                if operator == '':
                        operator = 'IN'
                raw_cond = input(f'''\nПоисковое слово или несколько слов
(через запятую с пробелом):
{operator} ''').split(', ')
                cond = ['"' + word + '"' for word in raw_cond]
                if operator == 'IN':
                        query.append(f'({col_name} {operator} ({", ".join(cond)}))')
                elif operator == 'NOT IN':
                        query.append(f'(NOT {col_name} IN ({", ".join(cond)}))')
                        
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
        #соответствующих индексируемых сжатых таблиц.
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
        
        #Открытие проиндексированного архива на чтение
        #и файла для поисковых результатов на запись.
        with gzip.open(os.path.join(arc_dir_path, arc_file_name), mode='rt') as arc_file_opened:
                with open(trg_file_path, 'w') as trg_file_opened:
                        
                        #Созданная бэкендом база включает в
                        #себя также набор элементов хэдера.
                        #Исходная последовательность этих
                        #элементов сохранена, поэтому из
                        #них легко собраем хэдер обратно.
                        cursor.execute('SELECT header_cells FROM header')
                        header_line = '\t'.join([tup[0] for tup in cursor])
                        
                        #Прописываем восстановленный хэдер в конечный файл.
                        trg_file_opened.write(header_line + '\n')
                        
                        #Создаём счётчик количества прописанных
                        #строк со стартовым значением, равным 1.
                        #По нему далее будет определено, оказались ли
                        #в конечном файле строки, отличные от хэдера.
                        num_of_lines = 1
                        
                        print(f'\nПоиск по таблице {tab_name} базы данных')
                        
                        #Инструкция, собственно, поиска.
                        #Собираем для неё запрос из списка
                        #сформированных ранее условий.
                        #Инструкция позволит извлечь из
                        #текущей таблицы БД байтовые позиции
                        #начала соответствующих этому
                        #запросу строк архивированной таблицы.
                        cursor.execute(f'''SELECT line_start FROM {tab_name}
                                           WHERE {" AND ".join(query)}''')
                        
                        #Перемещение курсора по сжатой таблице к
                        #началу каждой отвечающей запросу строки.
                        #Прописывание этих строк в конечный файл.
                        #Инкрементация счётчика прописанных строк.
                        for tup in cursor:
                                arc_file_opened.seek(int(tup[0]))
                                trg_file_opened.write(arc_file_opened.readline())
                                num_of_lines += 1
                                
        #Если счётчик так и остался равен единице,
        #значит, результатов поиска по данной таблице
        #нет, и в конечный файл попал только хэдер.
        #Такие конечные файлы программа удаляет.
        if num_of_lines == 1:
                os.remove(trg_file_path)
                
cnx.close()
