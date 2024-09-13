import requests

# yandex, vk, sber
EMPLOYERS = {"Яндекс": 1740, "VK": 15478, "СБЕР": 3529, "Ozon": 2180, "Авито": 84585,
             "АКАДО Телеком": 13820, "МТС": 3776, "Дом.ру": 10492215, "Skyeng": 1122462, "Технопарк": 26250}


def get_vacations_by_employers_id(employers: dict, pages=10) -> list:
    '''функция выдает список вакансий для компаний из апи хх ру'''
    result = []
    for employer in employers:
        employer_id = employers[employer]
        vacations = requests.get(f"https://api.hh.ru/vacancies?employer_id={employer_id}&only_with_salary=true&area=1").json()
        for vacation in vacations['items']:
            salary = vacation['salary']['from']
            name = vacation['name']
            url = vacation['alternate_url']
            employer_name = vacation['employer']['name']
            result.append({"salary": salary, "name": name, "url": url, "employer_name": employer_name})

    return result
