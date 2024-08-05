import unittest
import requests


class TestPost(unittest.TestCase):

    def test_request(self):
        url = "https://www.farpost.ru/vladivostok/realty/sell_flats/"
        result = requests.get(url).status_code
        self.assertEqual(result, 200, 'Сервер не отвечает')





if __name__ == '__main__':
    unittest.main()
