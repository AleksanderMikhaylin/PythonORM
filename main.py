import json
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()

class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=50), unique=True)

class Shop(Base):
    __tablename__ = "shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=50), unique=True)

class Book(Base):
    __tablename__ = "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=100), nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)

    publisher = relationship(Publisher, backref="books")

class Stock(Base):
    __tablename__ = "stock"

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    book = relationship(Book, backref="stocks")
    shop = relationship(Shop, backref="stocks")

class Sale(Base):
    __tablename__ = "sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.DECIMAL(15,2), nullable=False)
    date_sale = sq.Column(sq.DateTime, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    stock = relationship(Stock, backref="stocks")

def create_tables(engine):
    Base.metadata.create_all(engine)

def delete_tables(engine):
    Base.metadata.drop_all(engine)

if __name__ == '__main__':

    data_base = input('Введите имя БД>: ')
    user = input('Введите логин: ')
    password = input('Введите пароль: ')


    DSN = f"postgresql://{user}:{password}@localhost:5432/{data_base}"
    engine = sq.create_engine(DSN)
    # delete_tables(engine)
    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    dict_table = {
        "publisher": Publisher,
        "shop": Shop,
        "book": Book,
        "stock": Stock,
        "sale": Sale
    }

    dict_table_id = {
        "publisher": [],
        "shop": [],
        "book": [],
        "stock": [],
        "sale": []
    }
    for item in dict_table_id:
        q = session.query(dict_table[item].id)
        dict_table_id[item] = q.all()

    with open("data.json") as f:
        data = json.load(f)
        for item in data:
            if not (item.get('pk'),) in dict_table_id[item['model']]:
                session.add(dict_table[item['model']](id=item.get('pk'), **item.get('fields')))

        session.commit()

    # БД заполнена, можно делать выборки...
    while True:
        key = input('Введите имя или идентификатор издателя (publisher): ')
        if key == '':
            break

        q = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).join(Publisher).join(Stock).join(Shop).join(Sale)

        if key.isdigit():
            q = q.filter(Publisher.id == key)
        else:
            q = q.filter(Publisher.name.like(f'%{key}%'))

        for s in q.all():
            print(f"{s.title:<40} | {s.name:<10} | {s.price:<8} | {s.date_sale}")

    session.close()
