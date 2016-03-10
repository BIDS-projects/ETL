from .config import *
from .models import *

import sqlalchemy as sa
import sqlalchemy.orm as sao


class MySQL(object):
    """
    MySQL database connection abstraction.
    """

    def __init__(self, config):
        """
        Initialize connection.
        """
        self.engine = sa.create_engine(
            'mysql+pymysql://{username}:{password}@{host}/{database}'.format(
                username=config.username,
                password=config.password,
                host=config.host,
                database=config.database
            )
        )
        self.session = sao.scoped_session(sao.sessionmaker(bind=self.engine))

        # Set DB to self
        MySQLBase.db = self

        # Extra MySQL initialization
        MySQLBase.metadata.create_all(bind=self.engine)
