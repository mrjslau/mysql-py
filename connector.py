from mysql.connector import MySQLConnection, Error
from py_mysql_dbconfig import read_db_config

def connect():
    """ Connect to MySQL database """
    db_config = read_db_config()
 
    try:
        print('Connecting to MySQL database...')
        conn = MySQLConnection(**db_config)
        #Sukuriame nauja MySQLConnection
        #objekta - prisijungiame prie DB

        if conn.is_connected():
            print('connection established.')
            #Sukuriame nauja MySQLCursor objekta
            cursor = conn.cursor()

            ans = input('Would you like to create a new table?(y/n): ')
           
            if ans == 'y':
                create_table(cursor, 'reviewers')
                ans = 1
                while ans != 0:
                    ans = input('1 - print, 2 - add data, 3 - delete row, 4 - delete all rows, 5 - drop table, 0 - exit: ')
                    if ans == '1':
                        print_rows(cursor, 'reviewers')
                    elif ans == '2':
                        add_data(cursor, 'reviewers')
                    elif ans == '3':
                        print_rows(cursor)
                        row = int(input('Enter id of row: '))
                        delete_row(cursor, row)
                        print_rows(cursor)
                    elif ans == '4':
                        delete_all_rows(cursor)
                    else:
                        print('Table dropped.')
                        break
                drop_table(cursor, 'reviewers')
        else:
            print('connection failed.')
        
    except Error as error:
        print(error)
 
    finally:
        conn.close()
        print('Connection closed.')

def iter_row(cursor, size=10):
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row

def print_rows(cursor, name='reviewers'):
    #-------fetchmany()------
    cursor.execute("SELECT * FROM `reviewers`")
 
    for row in iter_row(cursor, 10):
        print(row)    

def create_table(cursor, name='reviewers'):
    cursor.execute("""CREATE TABLE IF NOT EXISTS`reviewers` (
                      `id` int(11) NOT NULL AUTO_INCREMENT,
                      `first_name` varchar(40) NOT NULL,
                      `last_name` varchar(40) NOT NULL,
                      `photo` blob,
                      PRIMARY KEY (`id`)
                      ) ENGINE=InnoDB AUTO_INCREMENT=128 DEFAULT CHARSET=latin1;""")
    print('Table created')

def add_data(cursor, name='reviewers'):
    cursor.execute("""INSERT INTO `reviewers`(`id`,`first_name`,`last_name`,`photo`)
                      VALUES (1,'Herbert','Methley ',NULL),
                      (2,'Olive','Wellwood ',NULL), 
                      (3,'Craig','The Doors ',NULL)""")
    print('Data added')

def delete_row(cursor, id, name='reviewers'):
    query = "DELETE FROM `reviewers` WHERE id = %s"
    cursor.execute(query, (id,))
    print(f'Row {id} deleted')

def delete_all_rows(cursor, name='reviewers'):
    cursor.execute("TRUNCATE TABLE `reviewers`")
    print('All rows are deleted')

def drop_table(cursor, name='reviewers'):
    cursor.execute("DROP TABLE `reviewers`;")
    print('Dropped')