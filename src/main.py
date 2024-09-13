from db_manager import DBManager
from hh_ru_parser import EMPLOYERS
from hh_ru_parser import get_vacations_by_employers_id


vacations = get_vacations_by_employers_id(EMPLOYERS)
db = DBManager("postgres", "Qazxc1234", "localhost", "5432")
print("создана база данных postgres_db")
db.create_tables_employers()
print("создана таблица компаний")
db.create_tables_vacations()
print("создана таблица вакансий")

for employer_name in EMPLOYERS:
    db.insert_into_employers_table(employer_name)
for vacation in vacations:
    db.insert_into_vacations_table(vacation['name'], vacation['salary'], vacation['url'], vacation['employer_name'])
print("вставили записи из апи хх ру в базу данных")
employers_vacation_count = db.get_companies_and_vacancies_count()
print("\n\nкомпании и количество их вакансий")
for name, count in employers_vacation_count:
    print("компания:", name, "количество вакансий:", count)

print("\n\nвсе вакансии с названием их компании")

all_vacations = db.get_all_vacancies()
for name, salary, url, employer in all_vacations:
    print("компания:", name, "зарплата:", salary, "ссылка:", url, "компания:", employer)

avg_salary = int(db.get_avg_salary())
print("\n\nсредняя зарплата по вакансиям:", avg_salary)


vacation_salary_high_avg = db.get_vacancies_with_higher_salary()
print("\n\nвакансии у которых зарплата выше средней")
for name, salary, url in vacation_salary_high_avg:
    print("вакансия:", name, "зарплата:", salary, "ссылка:", url)

vacation_with_keyword = db.get_vacancies_with_keyword("Хранитель")
print("\n\nвакансии с ключевым словом в названии")
for name, salary, url in vacation_with_keyword:
    print("вакансия:", name, "зарплата:", salary, "ссылка:", url)
