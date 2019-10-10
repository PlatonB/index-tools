__version__ = 'V1.0'

print('''
Программа, получающая характеристики элементов
выбранного столбца по нужным таблицам.

Автор: Платон Быкадоров (platon.work@gmail.com), 2019.
Версия: V1.0.
Лицензия: GNU General Public License version 3.
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/index-tools/blob/master/README.md

Обязательно!
Перед запуском программы нужно установить модуль:
pip3 install clickhouse-driver --user

Таблицы, по которым будет производиться аннотирование,
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
arc_dir_path, trg_top_dir_path, db_name, tab_names, col_names_n_types = create_database()

if len(col_names_n_types) > 1:
        col_name = input(f'''\nИмя столбца индексированных
таблиц, по которому аннотируем
[{"|".join(col_names_n_types.keys())}]: ''')
        if col_name not in col_names_n_types:
                print(f'{col_name} - недопустимая опция')
                sys.exit()
                
else:
        col_name = list(col_names_n_types.keys())[0]
        
ann_dir_path = os.path.normpath(input('\nПуть к папке с аннотируемыми таблицами: '))

num_of_headers = input('''\nКоличество не обрабатываемых строк
в начале каждой аннотируемой таблицы
(игнорирование ввода ==> производить работу для всех строк)
[0(|<enter>)|1|2|...]: ''')
if num_of_headers == '':
        num_of_headers = 0
else:
        num_of_headers = int(num_of_headers)
        
ann_col_index = int(input('\nНомер аннотируемого столбца: ')) - 1

#Стандартные действия
#для подключения к БД.
client = Client('localhost')
client.execute(f'USE {db_name}')

#Созданная бэкендом БД также
#содержит отдельную таблицу с
#элементами пантабличной шапки.
#Исходная последовательность этих
#элементов сохранена, поэтому из
#них легко собираем шапку обратно.
header_line = '\t'.join([tup[0] for tup in client.execute('SELECT header_cells FROM header')])

#Получение списка имён аннотируемых
#таблиц и перебор его элементов.
#Игнорирование имён скрытых бэкап-файлов,
#автоматически генерируемых LibreOffice.
ann_file_names = os.listdir(ann_dir_path)
for ann_file_name in ann_file_names:
        if ann_file_name.startswith('.~lock.'):
                continue
        
        #Открытие аннотируемого файла на чтение.
        with open(os.path.join(ann_dir_path, ann_file_name)) as ann_file_opened:
                
                print(f'\nАннотирование столбца таблицы {ann_file_name}')
                
                #Скипаем хэдеры аннотируемой таблицы.
                for header_index in range(num_of_headers):
                        ann_file_opened.readline()
                        
                #Аннотируемый столбец желательно
                #очистить от повторяющихся элементов.
                #Для этого создадим из него множество.
                #Если запрос планируется к столбцу
                #базы, содержащему строковые данные,
                #то каждое запрашиваемое слово
                #заключим в одинарные кавычки.
                #Для столбцов с числовым типом
                #такого действия не требуется.
                #Decimal-значения программа
                #не поддерживает, ибо остаётся
                #неясным предназначение такой фичи.
                if col_names_n_types[col_name] == 'String':
                        ann_set = set("'" + line.split('\n')[0].split('\t')[ann_col_index] + "'" for line in ann_file_opened)
                elif col_names_n_types[col_name] == 'Int64':
                        ann_set = set(line.split('\n')[0].split('\t')[ann_col_index] for line in ann_file_opened)
                else:
                        print('\nЧисла с точкой, имхо, бессмысленно аннотировать')
                        
        #Среди аннотируемых файлов
        #могут затесаться пустые.
        #Если их не проигнорировать, то
        #ClickHouse потом выдаст ошибку.
        if ann_set == set():
                print(f'\tЭтот файл пуст')
                continue
        
        #После того, как выяснилось, что аннотируемый
        #набор не пустой, создаём конечную подпапку.
        #Каждая подпапка предназначается для размещения
        #в ней результатов аннотирования одного файла.
        #Несмотря на то, что данные, подлежащие аннотации,
        #есть, не факт, что подпапка далее пригодится, ведь
        #в таблице БД может не найтись соответствующих значений.
        #Поэтому в конце этого цикла имеется код, очищающий
        #конечную папку от оставшихся пустыми подпапок.
        trg_dir_path = os.path.join(trg_top_dir_path, '.'.join(ann_file_name.split('.')[:-1]) + '_ann')
        os.mkdir(trg_dir_path)
        
        #Аннотирование каждого исходного файла
        #производится по всем таблицам базы.
        #Т.е. даже, если по одной из них
        #уже получились результаты, обход
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
                trg_file_name = f'ann_by_{".".join(arc_file_name.split(".")[:-1])}'
                trg_file_path = os.path.join(trg_dir_path, trg_file_name)
                
                #Создаём флаг, по которому далее будет
                #определено, оказались ли в конечном
                #файле строки, отличные от хэдеров.
                empty_res = True
                
                #Открытие проиндексированного архива на чтение
                #и файла для результатов аннотирования на запись.
                with gzip.open(os.path.join(arc_dir_path, arc_file_name)) as arc_file_opened:
                        with open(trg_file_path, 'w') as trg_file_opened:
                                
                                #Формируем и прописываем хэдер, повествующий
                                #о происхождении конечного файла.
                                trg_file_opened.write(f'##{ann_file_name} annotated by {arc_file_name}\n')
                                
                                #Прописываем восстановленную ранее
                                #шапку в качестве второго хэдера.
                                trg_file_opened.write(header_line + '\n')
                                
                                print(f'\n\tПоиск по таблице {tab_name} базы данных')
                                
                                #Инструкция поиска по столбцу
                                #таблицы БД, созданной быть
                                #источником характеристик
                                #аннотируемого столбца.
                                #Позволит извлечь из этой
                                #таблицы байтовые позиции
                                #начала содержащих запрашиваемые
                                #ячейки строк архивированной таблицы.
                                res = client.execute_iter(f'''SELECT line_start FROM {tab_name}
                                                              WHERE {col_name} IN ({", ".join(ann_set)})''')
                                
                                print(f'\tИзвлечение отобранных строк таблицы {arc_file_name}')
                                
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
                        
        #Программа также уничтожит оставшиеся
        #без конечных файлов подпапки.
        try:
                os.rmdir(trg_dir_path)
        except OSError:
                pass
        
client.disconnect()