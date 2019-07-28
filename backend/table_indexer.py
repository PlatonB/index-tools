__version__ = 'V1.3'

import os, mysql.connector, sys, gzip, copy, re

process_line = lambda arc_file_opened: arc_file_opened.readline().split('\n')[0].split('\t')

def fetch_cells(row, col_info):
        '''
        Функция принимает очередную строку обрабатываемой
        таблицы и словарь, ключи которого - названия
        выбранных столбцов, а значения - списки из типа
        данных и индекса ячейки каждого выбранного столбца.
        Из строки будут отобраны ячейки упомянутых столбцов.
        Для столбцов с текстовыми (строковыми) данными
        станет определена максимальная длина ячейки.
        '''
        cells = []
        for col_name, col_ann in col_info.items():
                
                #Если содержимое столбца исходной таблицы надо
                #разместить в БД в качестве столбца с числовым
                #типом данных, то для дальнейшего формирования
                #инструкции INSERT и выполнения CREATE INDEX
                #достаточно будет просто добавить ячейку исходного
                #столбца в возвращаемый описываемой функцией список.
                if col_ann[0] == 'INT' or col_ann[0].startswith('DECIMAL'):
                        cells.append(row[int(col_ann[1])])
                        
                #Строковые же ячейки, во-первых,
                #нужно заключать в кавычки.
                else:
                        cells.append(f'"{row[int(col_ann[1])]}"')
                        
                        #Во-вторых, индексируемые MySQL строковые
                        #данные должны иметь явно заданную длину.
                        #Если для строкового типа CHAR указать
                        #максимально разрешённую длину - 255 - то
                        #индексация больших таблиц будет очень долгой.
                        #Но реальную максимальную длину в пределах
                        #того или иного столбца мы по началу не знаем.
                        #Выход из положения - пополнять столбец
                        #БД CHAR-элементами длиной 255, по ходу
                        #определяя истинную максимальную длину.
                        #И уже потом можно будет для всего
                        #столбца сменить тип CHAR(255) на CHAR
                        #с полученным значением длины в скобках.
                        prev_cell_len = int(re.search(r'\d+', col_info[col_name][0]).group())
                        cell_len = len(row[int(col_ann[1])])
                        if cell_len > prev_cell_len or prev_cell_len == 255:
                                col_info[col_name][0] = re.sub(str(prev_cell_len), str(cell_len), col_info[col_name][0])
                                
        return cells

def create_database():
        '''
        Функция создаст MySQL-базу данных и
        пополнит каждую её таблицу информацией,
        обеспечивающей быстрый доступ к элементам
        соответствующей сжатой исходной таблицы.
        '''
        ind_dir_path = os.path.normpath(input('\nПуть к папке с индексируемыми архивами: '))
        
        trg_dir_path = input('\nПуть к папке для поисковых результатов: ')
        
        user = input('\nИмя пользователя в MySQL: ')
        
        #Имя базы данных сделаем для простоты тем
        #же, что и у папки с индексируемыми файлами.
        #Соединение с MySQL, создание объекта курсора,
        #и проверка на наличие базы, созданной по тем
        #же данным при прошлых запусках программы.
        db_name = os.path.basename(ind_dir_path)
        cnx = mysql.connector.connect(user=user)
        cursor = cnx.cursor()
        cursor.execute(f'SHOW DATABASES LIKE "{db_name}"')
        
        #Предыдущая база обнаружилась.
        #Вывод информации о хранимых там данных.
        if len([db_name for db_name in cursor]) == 1:
                print(f'\nБаза данных {db_name} уже существует')
                cursor.execute(f'USE {db_name}')
                cursor.execute('SHOW TABLES')
                tab_names = [tup[0] for tup in cursor if tup[0] != 'header']
                cursor.execute(f'''SHOW COLUMNS
                                   FROM {tab_names[0]}''')
                col_names_n_types = {tup[0]: tup[1].split('(')[0] for tup in cursor if tup[0] != 'line_start'}
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
                        cursor.execute(f'DROP DATABASE {db_name}')
                elif recreate in ['no', 'n', '']:
                        return ind_dir_path, trg_dir_path, user, db_name, tab_names, col_names_n_types
                else:
                        print(f'{recreate} - недопустимая опция')
                        sys.exit()
                        
        ram = int(input('\nОбъём оперативной памяти компьютера, Гбайт: '))
        
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
[integer(|i)|decimal(|d)|char(|c)]: ''')
                if data_type in ['integer', 'i']:
                        data_type = 'INT'
                elif data_type in ['decimal', 'd']:
                        tale = input('''\nСколько оставлять знаков после точки?
(игнорирование ввода ==> 5)
[...|5(|<enter>)|...|30): ''')
                        if tale == '':
                                tale = '5'
                        elif 0 > int(tale) > 30:
                                print(f'{tale} - недопустимая опция')
                                sys.exit()
                        data_type = f'DECIMAL(65, {tale})'
                elif data_type in ['char', 'c']:
                        data_type = 'CHAR(255)'
                else:
                        print(f'{data_type} - недопустимая опция')
                        sys.exit()
                col_info[col_name] = [data_type, 'cell_index']
                
                cont = input('''\nПроиндексировать по ещё одному столбцу?
(игнорирование ввода ==> нет)
[yes(|y)|no(|n|<enter>)]: ''')
                if cont not in ['yes', 'y', 'no', 'n', '']:
                        print('{cont} - недопустимая опция')
                        sys.exit()
                        
        #При дефолтной конфигурации MySQL размещение данных
        #в базу наборами по 10000 строк, как показала
        #практика, осуществляется крайне неэффективно,
        #или даже заканчивается вылетом программы.
        #На время процесса пополнения и индексации таблиц
        #БД приведём некоторые лимиты MySQL к оптимальным
        #для объёма RAM компьютера исследователя значениям.
        cursor.execute(f'SET GLOBAL innodb_buffer_pool_size = {str(ram // 2)} * 1024 * 1024 * 1024')
        cursor.execute(f'SET GLOBAL max_allowed_packet = {str(ram)} * 1024 * 1024 * 1024')
        
        #Создание БД, и выбор этой БД для использования
        #во всех последующих запросах по умолчанию.
        cursor.execute(f'CREATE DATABASE {db_name}')
        cursor.execute(f'USE {db_name}')
        
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
                        #со стороны MySQL), убираем его.
                        for header_cell_index in range(len(header_row)):
                                if header_row[header_cell_index].find('#') != -1:
                                        header_row[header_cell_index] = ''.join(header_row[header_cell_index].split('#'))
                                        
                        #На этапе пользовательского диалога
                        #был создан словарь с указанными
                        #пользователем именами столбцов и
                        #соответствующими MySQL-типами данных.
                        #Добавляем в этот словарь индексы имён этих
                        #столбцов, обозначающие их позицию в шапке.
                        #Эти же индексы будут определять
                        #положение соответствующих ячеек
                        #в каждой строке исходной таблицы.
                        for col_name in col_info.keys():
                                col_info[col_name][1] = str(header_row.index(col_name))
                                
                        #Для простоты назовём таблицы БД теми же
                        #именами, что и у исходных, но только без
                        #точек и дефисов, т.к. наличие таковых в
                        #именах таблиц MySQL-баз недопустимо.
                        tab_name = arc_file_name.replace('.', 'DOT').replace('-', 'DEFIS')
                        
                        print(f'\nТаблица {tab_name} новой базы данных пополняется')
                        
                        #Создаём таблицу БД, которая будет
                        #служить путеводителем по соответствующей
                        #gzip-архивированной исходной таблице.
                        #Имя и тип данных каждого столбца
                        #БД, кроме последнего, берём из
                        #ранее сформированного словаря.
                        #В качестве типа данных, отвечающего за
                        #текстовую информацию, в запрос пойдёт CHAR(255).
                        #Позже, как только выяснится, какова на самом
                        #деле максимальная длина строки, тип данных
                        #сменится на CHAR с меньшим значением в скобках.
                        #Последний столбец БД будет для
                        #позиций строк исходной таблицы.
                        #Поскольку они могут представлять
                        #собой огромные числа, то типом
                        #данных этого столбца сделаем BIGINT.
                        cursor.execute(f'''CREATE TABLE {tab_name}
                                           ({", ".join([col_name + " " + col_ann[0] for col_name, col_ann in col_info.items()])}, line_start BIGINT)''')
                        
                        #Данные будут поступать в
                        #базу одной или более порциями.
                        #Для контроля работы с порциями
                        #далее будет отмеряться их размер.
                        #Назначаем ноль в качестве
                        #стартового значения этой величины.
                        fragment_len = 0
                        
                        #Таблица БД будет пополняться
                        #до тех пор, пока не закончится
                        #перебор строк исходной таблицы.
                        while True:
                                
                                #Размер порции в 10000 был подобран эмпирически.
                                #Порции бОльших размеров, к сожалению, в процессе
                                #добавления в базу переполняют оперативную память.
                                if fragment_len == 10000:
                                        cursor.execute(f'''INSERT INTO {tab_name}
                                                       ({", ".join(col_info.keys())}, line_start)
                                                       VALUES{fragment}''')
                                        
                                        #После добавления порции
                                        #счётчик её размера обнуляется.
                                        fragment_len = 0
                                        
                                #Получение байтовой позиции начала
                                #текущей строки исходной таблицы.
                                #Устранение \n и разбиение
                                #этой строки на список.
                                line_start, row = str(arc_file_opened.tell()), process_line(arc_file_opened)
                                
                                #Чтение исходной таблицы завершено.
                                #Вероятнее всего, количество строк
                                #таблицы не кратно 10000, поэтому
                                #к этому моменту накопилось ещё
                                #некоторое количество данных.
                                #Допропишем тогда их в базу.
                                if row == ['']:
                                        if fragment_len > 0:
                                                cursor.execute(f'''INSERT INTO {tab_name}
                                                                   ({", ".join(col_info.keys())}, line_start)
                                                                   VALUES{fragment}''')
                                        break
                                
                                #Отбор ячеек тех столбцов исходной
                                #таблицы, по которым индексируем.
                                #Попутно будет сохраняться в словарь
                                #максимальная длина строковых ячеек
                                #(см. описание соответствующей функции).
                                cells = fetch_cells(row, col_info)
                                
                                #Добавление к списку отобранных
                                #ячеек байтовой позиции начала
                                #строки, содержащей эти ячейки.
                                #Эти позиции далее послужат
                                #индексом табличных строк.
                                cells.append(line_start)
                                
                                #Пополнение порции с нуля, в т.ч.
                                #после отправки в БД предыдущей,
                                #либо достройка текущей порции.
                                if fragment_len == 0:
                                        fragment = f'({", ".join(cells)})'
                                else:
                                        fragment += f', ({", ".join(cells)})'
                                        
                                #В любом случае, инкрементируем
                                #счётчик длины порции.
                                fragment_len += 1
                                
                print(f'Таблица {tab_name} новой базы данных индексируется')
                
                #Каждый столбец БД будет проиндексирован по-отдельности.
                #Проблем производительности при дальнейшем
                #выполнении комбинированных запросов наблюдаться
                #не должно, т.к. в современных версиях
                #MySQL задействуется алгоритм Index Merge.
                for col_name in col_info.keys():
                        
                        #Для строковых столбцов перед
                        #индексацией сменим CHAR(255)
                        #на CHAR с указанием реальной
                        #максимальной длины элемента.
                        if col_info[col_name][0].startswith('CHAR'):
                                cursor.execute(f'''ALTER TABLE {tab_name}
                                                   MODIFY {col_name} {col_info[col_name][0]}''')
                                col_info[col_name][0] = 'CHAR(255)'
                                
                        #Собственно, индексация.
                        cursor.execute(f'''CREATE INDEX {col_name}_index
                                           ON {tab_name} ({col_name})''')
                        
        #Соберём информацию об устройстве базы данных.
        #Она будет далее использоваться фронтендами.
        #Выведем также эти сведения на экран,
        #чтобы пользователю при запусках фронтендов
        #было очень легко в базе разобраться.
        cursor.execute('SHOW TABLES')
        tab_names = [tup[0] for tup in cursor]
        col_names_n_types = {col_name: col_ann[0].split('(')[0].lower() for col_name, col_ann in col_info.items()}
        print('\nТаблицы новой базы данных:\n', tab_names)
        print('\nСтолбцы таблиц и соответствующие типы данных новой БД:\n', col_names_n_types)
        
        #Общая для всех исходных таблиц шапка
        #направится в отдельную таблицу БД.
        #Каждая строка этой таблицы будет
        #состоять из элемента хэдера и
        #времени его размещения в базу.
        #Последнее нужно для сохранения
        #исходного порядка элементов.
        cursor.execute('''CREATE TABLE header
                          (header_cells CHAR(255), created TIMESTAMP)''')
        cursor.execute(f'''INSERT INTO header
                           (header_cells, created)
                           VALUES{", ".join(["('" + header_cell + "', NOW())" for header_cell in common_header_row])}''')
        cursor.execute('''CREATE INDEX header_index
                          ON header (created)''')
        
        cnx.close()
        
        return ind_dir_path, trg_dir_path, user, db_name, tab_names, col_names_n_types
