from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, BigInteger

Base = declarative_base()


class UserEvents(Base):
    __tablename__ = 'user_events'
    _id = Column(String, primary_key=True)
    _type = Column(String)
    date = Column(BigInteger)
    page = Column(String)
    device = Column(String)
    operating_system = Column(String)
    operating_system_version = Column(String)
    browser = Column(String)
    browser_version = Column(String)

    def __repr__(self):
        return "<UserEvents(_type='{}', date='{}', page={}, device={}, operating_system={}, " \
               "operating_system_version={}, browser={}, browser_version={})>" \
            .format(self._type, self.date, self.page, self.device, self.operating_system,
                    self.operating_system_version, self.browser, self.browser_version)
