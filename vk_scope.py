""" Модуль, отвечающий за:
    - авторизацию программы в качестве пользователя ВК,
    - создание экземпляров сущностей пользователя ВК, находящегося в диалоге с ботом,
    - создание экземпляров сущностей юзеров ВК, полученных в результате поиска
    - обработку результатов поиска.

    Дополнительно модуль имеет отдельный класс "VKGeoData" для сбора информации из базы данных ВК
    для её последующей записи в собственную БД программы. Нашел в просторах Гитхаба)"""

import json
import operator
import os
from typing import List, Dict, Any

import vk_api
from ratelimit import limits
from tqdm import tqdm

from database import Connect, User

LIST_OF_DICTS = List[Dict[str, Any]]


class VKAuth:
    # Введите токен пользователя
    TOKEN = 'HERE'
    if TOKEN:
        vk_session = vk_api.VkApi(token=TOKEN)
    else:
        username: str = os.getenv("VK_USER_LOGIN")
        password: str = os.getenv("VK_USER_PASS")
        scope = 'users,notify,friends,photos,status,notifications,offline,wall,audio,video'
        if not username or not password:
            username: str = input("Укажите свой логин: ")
            password: str = input("Укажите свой пароль: ")
        vk_session = vk_api.VkApi(username, password, scope=scope, api_version='5.124')


    try:
        vk_session.auth(token_only=True)
    except vk_api.AuthError as error_msg:
        print(error_msg)

class VKGeoData(VKAuth):
    """ Класс со служебными методами для сбора информации для БД """

    REQUESTS_PER = 3
    SECOND = 1

    def get_countries(self) -> LIST_OF_DICTS:
        """Служебный метод для сбора всех стран.
        Используется для заполнения БД."""

        print('Страны')
        countries = []
        countries_query = self.vk_session.method('database.getCountries',
                                                 values={'need_all': 1, 'count': 1000})['items']

        for country in countries_query:
            new_dic = {'model': 'country', 'fields': country}
            countries.append(new_dic)

        with open('../DB/Fixtures/countries.json', 'w', encoding='utf-8') as f:
            json.dump(countries, f)
        return countries_query

    @limits(REQUESTS_PER, SECOND)
    def get_regions(self, countries: LIST_OF_DICTS = None) -> LIST_OF_DICTS:
        """Служебный метод для сбора всех регионов во всех странах.
        Используется для заполнения БД."""

        print('Регионы')
        regions = [{'model': 'region', 'fields': {"id": 1, "title": "Москва город", "country_id": 1}},
                   {'model': 'region', 'fields': {"id": 2, "title": "Санкт-Петербург город", "country_id": 1}}]

        if not countries:
            try:
                with open('../DB/Fixtures/countries.json', 'r', encoding='utf-8') as f:
                    countries = json.load(f)
            except (FileNotFoundError, FileExistsError):
                countries = self.get_countries()
        for country in countries:
            print(".", end='')

            regions_quantity = \
                self.vk_session.method('database.getRegions', values={'country_id': country['fields']['id'],
                                                                      'count': 100})['count']

            if regions_quantity:
                search_values = {'country_id': country['fields']['id'], 'count': 100}
                regions_quantity = self.vk_session.method('database.getRegions', values=search_values)['count']
                if regions_quantity > 100:
                    queries = regions_quantity // 100 + 1
                    values = {'country_id': country['fields']['id'], 'count': 100, 'offset': 0}
                    for query in tqdm(range(queries), desc=f"Обходим регионы в стране {country['fields']['title']}"):
                        values['offset'] = 100 * query
                        regions_list = self.vk_session.method('database.getRegions', values=values)['items']
                        if regions_list:
                            for region in regions_list:
                                region.update({'country_id': country['fields']['id']})
                                new_dic = {'model': 'region', 'fields': region}
                                regions.append(new_dic)

                    else:
                        regions_list = self.vk_session.method('database.getRegions', values=search_values)['items']
                        if regions_list:
                            for region in regions_list:
                                region.update({'country_id': country['fields']['id']})
                                new_dic = {'model': 'region', 'fields': region}
                                regions.append(new_dic)

        with open('../DB/Fixtures/regions.json', 'w', encoding='utf-8') as f:
            json.dump(regions, f)
        return regions

    @limits(REQUESTS_PER, SECOND)
    def get_cities(self, regions: LIST_OF_DICTS = None) -> LIST_OF_DICTS:
        """Служебный метод для сбора всех городов во всех странах.
        Используется для заполнения БД."""

        print('Города')
        cities = []

        if not regions:
            try:
                with open('../DB/Fixtures/regions.json', 'r', encoding='utf-8') as f:
                    regions = json.load(f)
            except (FileNotFoundError, FileExistsError):
                regions = self.get_regions()
        for region in regions:
            print(".", end='')

            search_values = {'country_id': region['fields']['country_id'], 'region_id': region['fields']['id'],
                             'need_all': 1, 'count ': 100}
            cities_quantity = self.vk_session.method('database.getCities', values=search_values)['count']

            if cities_quantity:
                if cities_quantity > 100:
                    queries = cities_quantity // 100 + 1
                    values = {'country_id': region['fields']['country_id'], 'region_id': region['fields']['id'],
                              'offset': 0, 'need_all': 1, 'count ': 100}
                    for query in tqdm(range(queries), desc=f"Обходим города в регионе {region['fields']['title']}"):

                        values['offset'] = 100 * query
                        cities_list = self.vk_session.method('database.getCities', values=values)['items']
                        if cities_list:
                            for city in cities_list:
                                city.update({'region_id': region['fields']['id']})
                                new_dic = {'model': 'city', 'fields': city}
                                cities.append(new_dic)

                else:
                    cities_list = self.vk_session.method('database.getCities', values=search_values)['items']
                    if cities_list:
                        for city in cities_list:
                            city.update({'region_id': region['fields']['id']})
                            new_dic = {'model': 'city', 'fields': city}
                            cities.append(new_dic)

        with open('../DB/Fixtures/cities.json', 'w', encoding='utf-8') as f:
            json.dump(cities, f)
        return cities

class VKUser(VKAuth, Connect):
    """Класс пользователя ВК, общающегося с ботом"""

    def __init__(self, id: int):
        self.user_id = id
        info = self.get_self_info(self.user_id)
        self.first_name = info[0].get('first_name')
        self.last_name = info[0].get('last_name')
        self.sex = info[0].get('sex')
        self.link = 'https://vk.com/' + str(info[0].get('domain'))
        self.welcomed = False

        # Если город и страна пользователя не указаны - Москва по умолчанию
        if not info[0].get('city'):
            self.city = {'id': 1, 'title': 'Москва'}
            self.country = {'id': 1, 'title': 'Россия'}
        else:
            self.city = info[0].get('city')
            self.country = info[0].get('country')

    def get_self_info(self, user_id: int) -> LIST_OF_DICTS:
        """Метод получения всей необходимой информации о пользователе"""
        search_values = {
            'user_id': user_id,
            'fields': 'city, country, sex, domain, home_town'
        }
        return self.vk_session.method('users.get', values=search_values)

    def insert_self_to_db(self) -> None:
        """ Метод записи в БД информации о пользователе """
        fields = {
            'id': self.user_id,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'sex_id': self.sex,
            'city_id': self.city['id'],
            'link': self.link
        }

        # перепроверяем юзера на наличие в БД
        if not self.select_from_db(User.id, User.id == self.user_id).first():
            self.insert_to_db(User, fields)


class VKDatingUser(VKAuth):
    """Класс юзера ВК, найденного по запросу пользователя"""

    def __init__(self, db_id: int, vk_id: int, first_name: str, last_name: str, vk_link: str):
        self.db_id = db_id
        self.id = vk_id
        self.first_name = first_name
        self.last_name = last_name
        self.link = vk_link

    def __str__(self):
        return self.first_name + ' ' + self.last_name + ' ' + self.link

    def get_photo(self):
        """Метод получения топ-3 фото юзера"""
        search_values = {'owner_id': self.id, 'album_id': 'profile', 'count': 1000, 'extended': 1,
                         'photo_sizes': 1, 'type': 'm'}
        response = self.vk_session.method('photos.get', values=search_values)
        photos = []
        for photo in response['items']:
            photos.append((photo['id'], photo['owner_id'], photo['likes']['count']))
        sorted_photos = sorted(photos, key=operator.itemgetter(2), reverse=True)
        top3_photos = [(id, photo) for id, photo, _ in sorted_photos][:3]
        return top3_photos


if __name__ == '__main__':
    geo = VKGeoData()
    now = datetime.now()
    print(now)
    geo.get_countries()
    geo.get_regions()
    geo.get_cities()
    print(datetime.now() - now)
