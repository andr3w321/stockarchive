import unittest
import stockarchivedb as sa
import datetime
from dateutil.tz import tzutc

def get_last_sunday():
    today = datetime.date.today()
    idx = (today.weekday() + 1) % 7
    return today - datetime.timedelta(idx)

class TestStockarchive(unittest.TestCase):

    def test_implied_vol(self):
        self.assertEqual(round(sa.implied_volatility(10,100,100,2,0.01,'c'),2), 0.16)

    def test_dte(self):
        self.assertEqual(sa.get_dte("2020-11-20 00:00:00+00:00", datetime.datetime(2020, 11, 15, 23, 28, 3, 93688, tzinfo=tzutc())), 5.022186415648148)

    def test_was_market_open_today(self):
        # assume all weekdays market is open for test
        if datetime.datetime.today().weekday() in [0,1,2,3,4] and datetime.datetime.today().hour > 4:
            self.assertTrue(sa.was_market_open_today())
        else:
            self.assertFalse(sa.was_market_open_today())

    def test_was_market_open_sunday(self):
        self.assertFalse(sa.was_market_open(str(get_last_sunday())))

    # need a db for these tests to pass
    def test_get_risk_free_rate(self):
        #self.assertEqual(sa.get_risk_free_rate("2020-1-2"), 1.882)
        pass

if __name__ == "__main__":
    unittest.main()
