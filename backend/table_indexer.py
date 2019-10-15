__version__ = 'V2.3'

import os, sys, gzip, copy, re
from clickhouse_driver import Client
from decimal import Decimal, ROUND_HALF_UP

'''
Чтение очередной строки, отщепление
от неё переноса и разбиение на список.
'''
process_line = lambda arc_file_opened: arc_file_opened.readline().split('\n')[0].split('\t')

def fetch_cells(row, col_info, line_start):
        '''
        Функция принимает очередную строку обрабатываемой
        таблицы и словарь, ключи которого - названия
        выбранных столбцов, а значения - списки из типа
        данных, индекса ячейки и, если имеется, количества
        цифр после точки для каждого выбранного столбца.
        Последний элемент словаря - исключение: он
        описывает формируемый по ходу выполнения программы
        столбец с позициями начала нехэдерных табличных строк.
        Функция произведёт отбор ячеек упомянутых столбцов из
        текущей строки и присвоит каждой подходящий тип данных.
        '''
        cells = []
        for col_name, col_ann in col_info.items():
                
                #Добавление к списку отобранных
                #ячеек байтовой позиции начала
                #строки, содержащей эти ячейки.
                #Такие позиции далее послужат
                #индексом табличных строк.
                if col_name == 'line_start':
                        cells.append(line_start)
                        return cells
                
                #База далее должна будет пополняться с помощью
                #INSERT INTO, но при этом clickhouse-driver
                #позволяет подавать сами значения отдельным
                #Python-словарём, а не в составе INSERT-инструкции.
                #Поэтому отобранным ячейкам можно дать питоновские
                #типы данных: int для целых чисел, Decimal для
                #вещественных чисел и str, если ячейка нечисловая.
                #Вещественные числа будут округляться до заданного
                #исследователем количества знаков после точки.
                #Если следующая после этого количества цифра - 5,
                #то округление произведётся до большего значения.
                elif col_ann[0] == 'Int64':
                        cells.append(int(row[col_ann[1]]))
                elif col_ann[0].startswith('Decimal64'):
                        cells.append(Decimal(row[col_ann[1]]).quantize(Decimal('1.' + '0' * int(col_ann[2])),
                                                                       ROUND_HALF_UP))
                elif col_ann[0] == 'String':
                        cells.append(row[col_ann[1]])
                        
def create_database():
        '''
        Функция создаст ClickHouse-базу данных и
        пополнит каждую её таблицу информацией,
        обеспечивающей быстрый доступ к элементам
        соответствующей сжатой исходной таблицы.
        '''
        ind_dir_path = os.path.normpath(input('\nПуть к папке с индексируемыми архивами: '))
        
        trg_dir_path = input('\nПуть к папке для результатов: ')
        
        #Имя базы данных сделаем для простоты почти
        #тем же, что и у папки с индексируемыми файлами.
        #Соединение с ClickHouse, создание клиент-объекта.
        db_name = f'DBCH{os.path.basename(ind_dir_path)}'
        client = Client('localhost')
        
        #Проверка на наличие базы,
        #созданной по тем же данным
        #при прошлых запусках программы.
        #Если предыдущая БД обнаружилась, то
        #выведудся имена хранимых там таблиц
        #и столбцов, а также типы данных.
        if (f'{db_name}',) in client.execute('SHOW DATABASES'):
                print(f'\nБаза данных {db_name} уже существует')
                client.execute(f'USE {db_name}')
                tab_names = [tup[0] for tup in client.execute('SHOW TABLES') if tup[0] != 'header']
                table_struc = client.execute_iter(f'DESCRIBE TABLE {tab_names[0]}')
                col_names_n_types = {tup[0]: tup[1] for tup in table_struc if tup[0] != 'line_start'}
                print('\nТаблицы ранее созданной базы данных:\n', tab_names)
                print('\nСтолбцы таблиц и соответствующие типы данных ранее созданной БД:\n', col_names_n_types)
                
                #Раз БД, соответствующая таблицам
                #выбранной папки, ранее была создана,
                #то можно сразу приступать к её
                #эксплуатации с помощью фронтенда.
                #Иной вариант - создать базу заново,
                #чтобы, например, переиндексировать
                #эти таблицы по другим столбцам.
                recreate = input('''\nПересоздать базу данных?
[yes(|y)|no(|n|<enter>)]: ''')
                if recreate in ['yes', 'y']:
                        client.execute(f'DROP DATABASE {db_name}')
                elif recreate in ['no', 'n', '']:
                        return ind_dir_path, trg_dir_path, db_name, tab_names, col_names_n_types
                else:
                        print(f'{recreate} - недопустимая опция')
                        sys.exit()
                        
        ram = int(input('\nОбъём оперативной памяти компьютера, Гбайт: ')) * 1e9
        
        detect_headers = input('''\nРаспознавать хэдеры VCF (##) и UCSC (track_name=)
индексируемых таблиц автоматически, или потом
вы укажете количество хэдеров самостоятельно?
(Предпросмотрщик больших файлов есть в репозитории
https://github.com/PlatonB/bioinformatic-python-scripts)
[auto(|a)|manual(|m)]: ''')
        if detect_headers in ['auto', 'a']:
                num_of_unind = None
                
        elif detect_headers in ['manual', 'm']:
                num_of_unind = input('''\nКоличество не обрабатываемых строк
в начале каждой индексируемой таблицы
(Важно! Табулированную шапку к ним не причисляйте)
(игнорирование ввода ==> производить работу для всех строк)
[0(|<enter>)|1|2|...]: ''')
                if num_of_unind == '':
                        num_of_unind = 0
                else:
                        num_of_unind = int(num_of_unind)
        else:
                print(f'{detect_headers} - недопустимая опция')
                sys.exit()
                
        cont, col_info = 'y', {}
        while cont not in ['no', 'n', '']:
                col_name = input('''\nИмя индексируемого столбца
(Нужно соблюдать регистр)
[#Chrom|pos|RSID|...]: ''')
                col_name = ''.join(col_name.split('#'))
                
                data_type = input('''\nВ выбранном столбце - целые числа, вещественные числа или строки?
(примеры вещественного числа: 0.05, 2.5e-12)
(примеры строки: X, Y, A/C/G, rs11624464, HLA-DQB1)
[integer(|i)|decimal(|d)|string(|s)]: ''')
                if data_type in ['integer', 'i']:
                        data_type = 'Int64'
                        tale = None
                        
                elif data_type in ['decimal', 'd']:
                        tale = input('''\nСколько оставлять знаков после точки?
(игнорирование ввода ==> 5)
[...|5(|<enter>)|...|18): ''')
                        if tale == '':
                                tale = '5'
                        elif 0 > int(tale) > 18:
                                print(f'{tale} - недопустимая опция')
                                sys.exit()
                        data_type = f'Decimal64({tale})'
                        
                elif data_type in ['string', 's']:
                        data_type = 'String'
                        tale = None
                else:
                        print(f'{data_type} - недопустимая опция')
                        sys.exit()
                col_info[col_name] = [data_type, 'cell_index', tale]
                
                cont = input('''\nПроиндексировать по ещё одному столбцу?
(игнорирование ввода ==> нет)
[yes(|y)|no(|n|<enter>)]: ''')
                if cont not in ['yes', 'y', 'no', 'n', '']:
                        print('{cont} - недопустимая опция')
                        sys.exit()
                        
        #Доукомплектовываем созданный в рамках
        #пользовательского диалога словарь с названиями
        #и характеристиками выбранных пользователем
        #столбцов парой ключ-значение, описывающей
        #столбец индексов архивированной таблицы.
        col_info['line_start'] = ['Int64']
        
        #Получаем названия указанных пользователем
        #столбцов и столбца с индексами сжатой таблицы.
        col_names = list(col_info.keys())
        
        #ClickHouse не индексирует, а просто сортирует столбцы.
        #Выделим для сортировки половину оперативной памяти.
        #Если этого объёма не хватит, то ClickHouse задействует
        #внешнюю сортировку - размещение фрагментов столбца на
        #диске, сортировку каждого из них и поэтапное слияние.
        client.execute(f'SET max_bytes_before_external_sort = {int(ram) // 2}')
        
        #Создание БД, и выбор этой БД для использования
        #во всех последующих запросах по умолчанию.
        client.execute(f'CREATE DATABASE {db_name}')
        client.execute(f'USE {db_name}')
        
        print('')
        
        #Работа с архивами, каждый из
        #которых содержит по одной таблице.
        arc_file_names = os.listdir(ind_dir_path)
        for arc_file_name in arc_file_names:
                if arc_file_name.startswith('.~lock.'):
                        continue
                with gzip.open(os.path.join(ind_dir_path, arc_file_name), mode='rt') as arc_file_opened:
                        
                        #Автоматическое определение и прочтение
                        #вхолостую хэдеров таблиц распространённых
                        #биоинформатических форматов VCF и UCSC BED.
                        #Последний из прочитанных хэдеров (он
                        #же - шапка таблицы) будет сохранён.
                        if num_of_unind == None:
                                while True:
                                        header_row = process_line(arc_file_opened)
                                        if re.match(r'##|track_name=', header_row[0]) == None:
                                                break
                                        
                        #Холостое прочтение хэдеров, количество которых
                        #указано пользователем, и сохранение шапки.
                        else:
                                for unind_index in range(num_of_unind):
                                        arc_file_opened.readline()
                                header_row = process_line(arc_file_opened)
                                
                        #Обязательное требование программы -
                        #единообразие исходных таблиц.
                        #Доказательством соблюдения этого правила
                        #будет считаться одинаковость шапок.
                        #Шапка первой обрабатываемой таблицы
                        #назначается референсной, а шапки
                        #следующих таблиц будут с ней сопоставляться.
                        if 'common_header_row' not in locals():
                                common_header_row = copy.deepcopy(header_row)
                        elif header_row != common_header_row:
                                print('Шапки индексируемых таблиц не совпадают')
                                sys.exit()
                                
                        #Элементы шапки, озаглавливающие
                        #выбранные пользователем столбцы,
                        #станут потом именами столбцов БД.
                        #Поскольку в этих именах не должно
                        #быть символа # (таковы требования
                        #со стороны ClickHouse), убираем его.
                        for header_cell_index in range(len(header_row)):
                                if header_row[header_cell_index].find('#') != -1:
                                        header_row[header_cell_index] = ''.join(header_row[header_cell_index].split('#'))
                                        
                        #На этапе пользовательского диалога был
                        #создан словарь с указанными пользователем
                        #именами будущих столбцов БД и соответствующими
                        #поддерживаемыми ClickHouse типами данных.
                        #Добавляем ко всем ключам словаря,
                        #кроме отвечающего за столбец стартов
                        #строк, индексы имён этих столбцов,
                        #обозначающие их позицию в шапке.
                        #Эти же индексы будут определять
                        #положение соответствующих ячеек в
                        #каждой строке исходной таблицы.
                        for col_name in col_names[:-1]:
                                col_info[col_name][1] = header_row.index(col_name)
                                
                        #Для простоты назовём таблицы БД теми же
                        #именами, что и у исходных, но только без
                        #точек и дефисов, т.к. наличие таковых в
                        #именах таблиц ClickHouse-баз недопустимо.
                        #Таблицам также нельзя присваивать имена,
                        #начинающиеся с цифры, поэтому добавим
                        #каждому имени буквенную приставку.
                        tab_name = 'TBL' + arc_file_name.replace('.', 'DOT').replace('-', 'DEFIS')
                        
                        #Создаём таблицу БД, которая после
                        #дальнейшего заполнения будет служить
                        #путеводителем по соответствующей
                        #gzip-архивированной крупной таблице.
                        #Имя и тип данных каждого столбца БД
                        #берём из ранее сформированного словаря.
                        #По умолчанию ClickHouse сжимает каждый
                        #столбец очень быстрым, но практически
                        #не уменьшающим размер алгоритмом LZ4.
                        #Применем к столбцам оптимальный по
                        #скорости и степени компрессии Zstandart.
                        client.execute(f'''CREATE TABLE {tab_name}
                                           ({", ".join([col_name + " " + col_ann[0] + " CODEC(ZSTD(22))" for col_name, col_ann in col_info.items()])})
                                           ENGINE = MergeTree()
                                           ORDER BY ({", ".join(col_names[:-1])})''')
                        
                        print(f'Таблица {tab_name} новой базы данных пополняется')
                        
                        #Данные будут поступать в
                        #базу одной или более порциями.
                        #Для контроля работы с порциями
                        #далее будет отмеряться их размер.
                        #Назначаем ноль в качестве
                        #стартового значения этой величины.
                        fragment, fragment_len = [], 0
                        
                        #Таблица БД будет пополняться
                        #до тех пор, пока не закончится
                        #перебор строк исходной таблицы.
                        while True:
                                
                                #Размер порции в 100000 строк
                                #соответствует рекомендации из
                                #официальной документации ClickHouse.
                                if fragment_len == 100000:
                                        client.execute(f'''INSERT INTO {tab_name}
                                                           ({", ".join(col_names)})
                                                           VALUES''',
                                                       fragment)
                                        
                                        #После добавления порции список,
                                        #её содержащий, очищается, а
                                        #счётчик её размера обнуляется.
                                        fragment.clear()
                                        fragment_len = 0
                                        
                                #Получение байтовой позиции начала
                                #текущей строки исходной таблицы.
                                #Устранение \n и разбиение
                                #этой строки на список.
                                line_start, row = arc_file_opened.tell(), process_line(arc_file_opened)
                                
                                #Чтение исходной таблицы завершено.
                                #Вероятнее всего, количество строк
                                #таблицы не кратно 100000, поэтому
                                #к этому моменту накопилось ещё
                                #некоторое количество данных.
                                #Допропишем тогда их в базу.
                                if row == ['']:
                                        if fragment_len > 0:
                                                client.execute(f'''INSERT INTO {tab_name}
                                                                   ({", ".join(col_names)})
                                                                   VALUES''',
                                                               fragment)
                                        break
                                
                                #Отбор ячеек тех столбцов сжатой
                                #таблицы, по которым индексируем.
                                #Сохранение этих ячеек и стартовых
                                #позиций табличных строк в список.
                                cells = fetch_cells(row, col_info, line_start)
                                
                                #Пополнение порции с нуля, в т.ч.
                                #после отправки в БД предыдущей,
                                #либо достройка текущей порции.
                                fragment.append(dict(zip(col_names, cells)))
                                
                                #В любом случае, инкрементируем
                                #счётчик размера порции.
                                fragment_len += 1
                                
        #Соберём информацию об устройстве базы данных.
        #Она будет далее использоваться фронтендами.
        #Выведем также эти сведения на экран,
        #чтобы пользователю при запусках фронтендов
        #было очень легко в базе разобраться.
        tab_names = [tup[0] for tup in client.execute('SHOW TABLES')]
        col_names_n_types = {col_name: col_ann[0] for col_name, col_ann in col_info.items() if col_name != 'line_start'}
        print('\nТаблицы новой базы данных:\n', tab_names)
        print('\nСтолбцы таблиц и соответствующие типы данных новой БД:\n', col_names_n_types)
        
        #Общая для всех исходных таблиц шапка
        #направится в отдельную таблицу БД.
        client.execute('''CREATE TABLE header
                          (header_cells String)
                          ENGINE = TinyLog''')
        client.execute(f'''INSERT INTO header
                           (header_cells)
                           VALUES''',
                       [{'header_cells': header_cell} for header_cell in common_header_row])
        
        client.disconnect()
        
        return ind_dir_path, trg_dir_path, db_name, tab_names, col_names_n_types
