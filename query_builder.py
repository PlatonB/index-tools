__version__ = 'V3.0'

print('''
Конструктор запросов к большим таблицам.

Автор: Платон Быкадоров (platon.work@gmail.com), 2019.
Версия: V3.0.
Лицензия: GNU General Public License version 3.
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/index-tools/blob/master/README.md

Обязательно!
Перед запуском программы нужно установить модуль:
pip3 install clickhouse-driver --user

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

def create_todecimal_func(col_names_n_types, col_name, raw_cond):
        '''
        Если в стоблце БД - Decimal-значения,
        то и числа, подаваемые в запрос, надо
        конвертировать в этот тип данных.
        Эта функция конструирует функцию
        ClickHouse, выполняющую такую задачу.
        '''
        if col_names_n_types[col_name].startswith('Decimal'):
                cond = [f'toDecimal64({num}, {str(len(num.split(".")[1]))})' for num in raw_cond]
        else:
                cond = raw_cond
        return cond

####################################################################################################

print('\nИмпорт модулей программы...')

import sys

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

from backend.table_indexer import create_database
from clickhouse_driver import Client
import os, gzip

#Индексация выбранных пользователем
#столбцов исходных сжатых таблиц.
#При этом образуется БД, включающая
#в себя содержимое этих столбцов и
#индексы, представляющие собой байтовые
#позиции начала всех табличных строк.
#Получение путей к папке с упомянутыми
#таблицами и конечной папки, а также
#различных характеристик базы данных.
arc_dir_path, trg_dir_path, db_name, tab_names, col_names_n_types = create_database()

#Стандартные действия для подключения к БД.
client = Client('localhost')
client.execute(f'USE {db_name}')

cont, conds = 'y', []
while cont not in ['no', 'n', '']:
        
        if len(col_names_n_types) > 1:
                col_name = input('\nИмя столбца, в котором ищем: ')
                if col_name not in col_names_n_types:
                        print(f'{col_name} - недопустимая опция')
                        sys.exit()
                        
        else:
                col_name = list(col_names_n_types.keys())[0]
                
        if col_names_n_types[col_name] == 'String':
                operator = input('''\nОператор сравнения
(игнорирование ввода ==> поиск будет по самим словам)
[not in|in(|<enter>)]: ''').upper()
                if operator == '':
                        operator = 'IN'
                elif operator not in ['NOT IN', 'IN']:
                        print(f'{operator} - недопустимая опция')
                        sys.exit()
                        
                raw_cond = input(f'''\nИскомое/исключаемое слово или несколько слов
(через запятую с пробелом):
{operator} ''').split(', ')
                cond = [f"'{word}'" for word in raw_cond]
                conds.append(f'({col_name} {operator} ({", ".join(cond)}))')
                
        else:
                operator = input('''\nОператор сравнения
(игнорирование ввода ==> поиск будет по самим числам)
(between - диапазон от 1-го до 2-го числа *включительно*)
[>|<|>=|<=|between|not in|in(|<enter>)]: ''').upper()
                if operator == '':
                        operator = 'IN'
                elif operator not in ['>', '<', '>=', '<=', 'BETWEEN', 'NOT IN', 'IN']:
                        print(f'{operator} - недопустимая опция')
                        sys.exit()
                        
                if operator in ['NOT IN', 'IN']:
                        raw_cond = input(f'''\nИскомое/исключаемое число или несколько чисел
(через запятую с пробелом):
{operator} ''').split(', ')
                        cond = create_todecimal_func(col_names_n_types, col_name, raw_cond)
                        conds.append(f'({col_name} {operator} ({", ".join(cond)}))')
                        
                else:
                        if operator in ['>', '<', '>=', '<=']:
                                raw_cond = [input(f'''\nПоисковое условие:
{operator} ''')]
                                
                        elif operator == 'BETWEEN':
                                raw_cond = [input('\nНижняя граница: ')]
                                raw_cond.append(input('\nВерхняя граница: '))
                                
                        cond = create_todecimal_func(col_names_n_types, col_name, raw_cond)
                        conds.append(f'({col_name} {operator} {" AND ".join(cond)})')
                        
        if len(col_names_n_types) > 1:
                cont = input('''\nИскать ещё в одном столбце?
(игнорирование ввода ==> нет)
[yes(|y)|no(|n|<enter>)]: ''')
                if cont not in ['yes', 'y', 'no', 'n', '']:
                        print(f'{cont} - недопустимая опция')
                        sys.exit()
                        
        else:
                break
        
#Объединяем сформированные
#ранее поисковые условия.
where = " AND ".join(conds)

#Созданная бэкендом БД также
#содержит отдельную таблицу с
#элементами пантабличной шапки.
#Исходная последовательность этих
#элементов сохранена, поэтому из
#них легко собираем шапку обратно.
header_line = '\t'.join([tup[0] for tup in client.execute('SELECT header_cells FROM header')])

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
        arc_file_name = tab_name[3:].replace('DOT', '.').replace('DEFIS', '-')
        
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
                        
                        #Прописываем сконкатенированные
                        #условия поиска в конечный файл
                        #в качестве первого из хэдеров.
                        trg_file_opened.write(f'##{where}\n')
                        
                        #То же самое делаем с
                        #восстановленной ранее шапкой.
                        #Это будет второй хэдер.
                        trg_file_opened.write(header_line + '\n')
                        
                        print(f'\nПоиск по таблице {tab_name} базы данных')
                        
                        #Инструкция, собственно, поиска.
                        #Она позволит извлечь из текущей
                        #таблицы БД байтовые позиции начала
                        #отвечающих поисковым условиям
                        #строк архивированной таблицы.
                        res = client.execute_iter(f'''SELECT line_start FROM {tab_name}
                                                      WHERE {where}''')
                        
                        print(f'Извлечение отобранных строк таблицы {arc_file_name}')
                        
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
                        for line_start in res:
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
                
client.disconnect()
