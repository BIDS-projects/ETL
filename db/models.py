from sqlalchemy.dialects import mysql
from sqlalchemy.orm import relationship
import sqlalchemy as sa
import sqlalchemy.ext.declarative as sad


class Base(object):
    """
    Base specification for all objects.
    """

    def save(self):
        """
        Saves object in place.
        """
        raise NotImplementedError()

    def delete(self):
        """
        Deletes object.
        """
        raise NotImplementedError()


class MySQLBase(sad.declarative_base(), object):
    """
    The MySQL base object.
    """

    __abstract__ = True
    db = None

    id = sa.Column(sa.Integer, primary_key=True)

    @classmethod
    def objects(cls, give_query=False, **data):
        """
        Returns the database objects.
        """
        query = cls.query().filter_by(**data)
        return query if give_query else query.all()

    @classmethod
    def query(cls):
        """
        Returns the query object.
        """
        return cls.db.session.query(cls)

    def save(self):
        """
        Saves object to database.
        """
        try:
            self.db.session.add(self)
            self.db.session.commit()
            return self
        except AttributeError:
            import pdb; pdb.set_trace()


class FromItem(MySQLBase):
    """
    MySQL From object
    """

    __tablename__ = 'fromitem'

    id = sa.Column(sa.Integer, primary_key=True)
    base_url = sa.Column(mysql.BLOB())
    to_items = relationship("ToItem", backref='fromitem', uselist=True)

class ToItem(MySQLBase):
    """
    MySQL To object
    """

    __tablename__ = 'toitem'

    id = sa.Column(sa.Integer, primary_key = True)
    base_url = sa.Column(mysql.BLOB())
    from_id = sa.Column(sa.Integer, sa.ForeignKey(FromItem.id), nullable=False)
    from_item = relationship("FromItem", foreign_keys=[from_id])

class LinkItem(MySQLBase):
    """
    MySQL link object.
    """

    __tablename__ = 'links'

    id = sa.Column(sa.Integer, primary_key=True)
    base_url = sa.Column(sa.String(100))
    researchers = relationship("ResearcherItem", backref='links', uselist=True)

class ResearcherItem(MySQLBase):
    """
    MySQL Researcher object
    """

    __tablename__ = "names"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(mysql.BLOB())
    link_id = sa.Column(sa.Integer, sa.ForeignKey(LinkItem.id), nullable=False)
    domain = sa.Column(sa.String(100))
    link = relationship("LinkItem", foreign_keys=[link_id])
