import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import Error


class DBManager:
    def __init__(self, user, password, host, port) -> None:
        '''создание базы данных postgres_db если ее нет'''
        self.user = user
        self.__password = password
        self.host = host
        self.port = port
        # Подключение к существующей базе данных
        connection = psycopg2.connect(user=user,
                                      # пароль, который указали при установке PostgreSQL
                                      password=password,
                                      host=host,
                                      port=port)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        # получаем курсор, который позволяет выполнять SQL команды
        cursor = connection.cursor()
        sql_create_database = 'create database postgres_db;'
        try:
            cursor.execute(sql_create_database)
        except Error:
            pass
        finally:
            if connection:
                cursor.close()
                connection.close()

    def create_tables_employers(self) -> None:
        '''метод создает таблицу компаний'''
        connection = psycopg2.connect(user=self.user,
                                      # пароль, который указали при установке PostgreSQL
                                      password=self.__password,
                                      host=self.host,
                                      port=self.port,
                                      database="postgres_db")
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE employers
                                (id INT PRIMARY KEY NOT NULL,
                                 name VARCHAR(255) NOT NULL);'''
        try:
            cursor.execute(create_table_query)
            connection.commit()
        except Error:
            pass
        finally:
            if connection:
                cursor.close()
                connection.close()

    def create_tables_vacations(self) -> None:
        '''метод создает таблицу вакансий'''
        connection = psycopg2.connect(user=self.user,
                                      # пароль, который указали при установке PostgreSQL
                                      password=self.__password,
                                      host=self.host,
                                      port=self.port,
                                      database="postgres_db")
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE vacations
                                (id INT PRIMARY KEY NOT NULL,
                                 name VARCHAR(255) NOT NULL,
                                 salary INT NOT NULL,
                                 url TEXT,
                                 employer_id INT,
                                 FOREIGN KEY (employer_id) REFERENCES  Employers (id));'''
        try:
            cursor.execute(create_table_query)
            connection.commit()
        except Error:
            pass
        finally:
            if connection:
                cursor.close()
                connection.close()

    def insert_into_employers_table(self, employer_name) -> None:
        '''метод добавляет запись в таблицу компаний'''
        connection = psycopg2.connect(user=self.user,
                                      # пароль, который указали при установке PostgreSQL
                                      password=self.__password,
                                      host=self.host,
                                      port=self.port,
                                      database="postgres_db")
        cursor = connection.cursor()
        cursor.execute("SELECT MAX(id) FROM employers")
        max_current_id = cursor.fetchone()[0]
        max_current_id = max_current_id if max_current_id else 0
        new_id = max_current_id + 1
        try:
            cursor.execute(f"SELECT id FROM employers WHERE name = '{employer_name}'")
            employer_id = cursor.fetchone()
            if not employer_id:
                insert_in_table_query = f'''INSERT INTO employers (id, name)
                                            VALUES ({new_id}, '{employer_name}')'''
                cursor.execute(insert_in_table_query)
                connection.commit()
        except Error:
            pass
        finally:
            if connection:
                cursor.close()
                connection.close()

    def insert_into_vacations_table(self, name, salary, url, employer_name) -> None:
        '''метод добавляет запись в таблицу вакансий'''
        connection = psycopg2.connect(user=self.user,
                                      # пароль, который указали при установке PostgreSQL
                                      password=self.__password,
                                      host=self.host,
                                      port=self.port,
                                      database="postgres_db")
        cursor = connection.cursor()
        cursor.execute(f"SELECT id FROM employers WHERE name = '{employer_name}'")
        employer_id = cursor.fetchone()
        try:
            if employer_id:
                cursor.execute("SELECT MAX(id) FROM vacations")
                max_current_id = cursor.fetchone()[0]
                max_current_id = max_current_id if max_current_id else 0
                new_id = max_current_id + 1
                insert_in_table_query = f'''INSERT INTO vacations (id, name, salary, url, employer_id)
                                            VALUES ({new_id}, '{name}', {salary}, '{url}', {employer_id[0]})'''
                cursor.execute(insert_in_table_query)
                connection.commit()
        except Error:
            pass
        finally:
            if connection:
                cursor.close()
                connection.close()

    def get_companies_and_vacancies_count(self) -> list:
        '''метод выдает записи компаний с количеством их вакансий'''
        connection = psycopg2.connect(user=self.user,
                                      # пароль, который указали при установке PostgreSQL
                                      password=self.__password,
                                      host=self.host,
                                      port=self.port,
                                      database="postgres_db")
        cursor = connection.cursor()
        select_result = None
        try:
            select_query = '''SELECT employers.name, COUNT(*) AS vacation_count FROM employers
                              INNER JOIN vacations
                                ON employers.id = vacations.employer_id
                              GROUP BY employers.name'''
            cursor.execute(select_query)
            select_result = cursor.fetchall()
        except Error:
            pass
        finally:
            if connection:
                cursor.close()
                connection.close()
        return select_result

    def get_all_vacancies(self) -> list:
        '''метод выдает все вакансии с компаниями'''
        connection = psycopg2.connect(user=self.user,
                                      # пароль, который указали при установке PostgreSQL
                                      password=self.__password,
                                      host=self.host,
                                      port=self.port,
                                      database="postgres_db")
        cursor = connection.cursor()
        select_result = None
        try:
            select_query = '''SELECT vacations.name AS vacation_name, salary, url, employers.name AS employer_name 
                              FROM employers
                              INNER JOIN vacations
                                ON employers.id = vacations.employer_id'''
            cursor.execute(select_query)
            select_result = cursor.fetchall()
        except Error:
            pass
        finally:
            if connection:
                cursor.close()
                connection.close()
        return select_result

    def get_avg_salary(self) -> int:
        '''метод выдает сренюю зарплату по вакансиям'''
        connection = psycopg2.connect(user=self.user,
                                      # пароль, который указали при установке PostgreSQL
                                      password=self.__password,
                                      host=self.host,
                                      port=self.port,
                                      database="postgres_db")
        cursor = connection.cursor()
        select_result = None
        try:
            select_query = '''SELECT AVG(salary) as avg_salary FROM vacations'''
            cursor.execute(select_query)
            select_result = cursor.fetchone()
        except Error:
            pass
        finally:
            if connection:
                cursor.close()
                connection.close()
        return int(select_result[0])

    def get_vacancies_with_higher_salary(self) -> list:
        '''метод выдает вакансии, чья зарплата выше средней'''
        avg_salary = int(self.get_avg_salary())
        connection = psycopg2.connect(user=self.user,
                                      # пароль, который указали при установке PostgreSQL
                                      password=self.__password,
                                      host=self.host,
                                      port=self.port,
                                      database="postgres_db")
        cursor = connection.cursor()
        select_result = None
        try:
            select_query = f'''SELECT name, salary, url FROM vacations
                               WHERE salary > {avg_salary}'''
            cursor.execute(select_query)
            select_result = cursor.fetchall()
        except Error:
            pass
        finally:
            if connection:
                cursor.close()
                connection.close()
        return select_result

    def get_vacancies_with_keyword(self, key_word) -> list:
        '''метод выдает вакансии в названии которых есть ключевое слово'''
        connection = psycopg2.connect(user=self.user,
                                      # пароль, который указали при установке PostgreSQL
                                      password=self.__password,
                                      host=self.host,
                                      port=self.port,
                                      database="postgres_db")
        cursor = connection.cursor()
        select_result = None
        try:
            select_query = f"""SELECT name, salary, url FROM vacations
                               WHERE name LIKE '%{key_word}%'"""
            cursor.execute(select_query)
            select_result = cursor.fetchall()
        except Error:
            pass
        finally:
            if connection:
                cursor.close()
                connection.close()
        return select_result
