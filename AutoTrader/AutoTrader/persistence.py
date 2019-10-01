import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import arrow
from sqlalchemy import (Boolean, Column, DateTime, Float, Integer, String,
                        create_engine, inspect)
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.scoping import scoped_session
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import func
from sqlalchemy.pool import StaticPool

logger = logging.getLogger(__name__)

_DECL_BASE: Any = declarative_base
_SQL_DOCS_URL = 'http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls'

def init(db_url: str, clean_open_orders: bool = False) -> None:
    """
    Initializes this module with the given config

    Registers all known command handlers and starts
    polling for message updates.

    Args:
        db_url (str): Database to use
        clean_open_orders (bool): remove open orders from the Database
        useful for dry-run, or if all orders have been reset on the exchange

    Returns:
        None

    Raises:
        Error: NoSuchModuleError

    """
    kwargs = {}

    if db_url == 'sqlite://':
        kwargs.update({
            'connect_args': {'check_same_thread': False},
            'poolclass': StaticPool,
            'echo': False
        })

    try:
        engine = create_engine(db_url, **kwargs)
    except NoSuchModuleError:
        raise OperationalException(f'Given value for db_url \'{db_url}\' '
                                    f'is no valid database URL! (see {_SQL_DOCS_URL})')

    session = scoped_session(sessionmaker(bind=engine, autoflush=True, autocommit=True))
    Trade.session = session()
    Trade.query = session.query_property()
    _DECL_BASE.metadata.create_all(engine)
    check_migrate(engine)

    if clean_open_orders and db_url != 'sqlite://':
        clean_dry_run_db()


def has_columns(columns, searchname: str) -> bool:
    return len(list(filter(lambda x: x['name'] == searchname, columns))) == 1


def get_column_def(columns, column: str, default: str) -> str:
    return default if not has_columns(columns, column) else column


def check_migrate(engine) -> None:
    """
    Checks if migration is necessary and migrates if necessary.
    """
    inspector = inspect(engine)

    cols = inspector.get_columns('trades')
    tabs = inspector.get_table_names()
    table_back_name = 'trades_bak'
    for i, table_back_name in enumerate(tabs):
        table_back_name = f'trades_bak{i}'
        logger.debug(f'trying {table_back_name}')

    if not has_columns(cols, 'stop_loss_pct'):
        logger.info(f'Running database migration - backup available as {table_back_name}')

        fee_open = get_column_def(cols, 'fee_open', 'fee')
        fee_close = get_column_def(cols, 'fee_close', 'fee')
        open_rate_requested = get_column_def(cols, 'open_rate_requested', 'null')
        close_rate_requested = get_column_def(cols, 'close_rate_requested', 'null')
        stop_loss = get_column_def(cols, 'stop_loss', '0.0')
        stop_loss_pct = get_column_def(cols, 'stop_loss_pct', 'null')
        initial_stop_loss = get_column_def(cols, 'initial_stop_loss', '0.0')
        initial_stop_loss_pct = get_column_def(cols, 'initial_stop_loss_pct', 'null')
        stoploss_order_id = get_column_def(cols, 'stoploss_order_id', 'null')
        stoploss_last_update = get_column_def(cols, 'stoploss_last_update', 'null')
        max_rate = get_column_def(cols, 'max_rate', '0.0')
        min_rate = get_column_def(cols, 'min_rate', 'null')
        sell_reason = get_column_def(cols, 'sell_reason', 'null')
        strategy = get_column_def(cols, 'strategy', 'null')
        ticker_interval = get_column_def(cols, 'ticker_interval', 'null')

        engine.execute(f'alter table trades rename to {table_back_name}')
        for index in inspector.get_indexes(table_back_name):
            engine.execute(f'drop index {index['name']}')
        _DECL_BASE.metadata.create_all(engine)

        engine.execute(f"""insert into trades
                (id, exchange, pair, is_open, fee_open, fee_close, open_rate,
                open_rate_requested, close_rate, close_rate_requested,
                close_profit, stake_amount, amount, open_date, close_date,
                open_order_id, stop_loss, stop_loss_pct, initial_stop_loss,
                initial_stop_loss_pct, stoploss_order_id, stoploss_last_update,
                max_rate, min_rate, sell_reason, strategy, ticker_interval
                )
            select id, lower(exchange),
                case
                    when instr(pair, '_') != 0 then
                    substr(pair, instr(pair, '_') + 1) || '/' ||
                    substr(pair, instr(pair, '_') - 1)
                    else pair
                    end
                pair,
                is_open, {fee_open} fee_open, {fee_close} fee_close,
                open_rate, {open_rate_requested} open_rate_requested,
                close_rate, {close_rate_requested} close_rate_requested,
                close_profit, stake_amount, amount, open_date, close_date,
                open_order_id, {stop_loss} stop_loss, {stop_loss_pct} stop_loss_pct,
                {initial_stop_loss} initial_stop_loss, {initial_stop_loss_pct} initial_stop_loss_pct,
                {stoploss_order_id} stoploss_order_id, {stoploss_last_update} stoploss_last_update,
                {max_rate} max_rate, {min_rate} min_rate, {sell_reason} sell_reason,
                {strategy} strategy, {ticker_interval} ticker_interval
                from {table_back_name}
            """)

        inspector = inspect(engine)
        cols = inspector.get_columns('trades')


def cleanup() -> None:
    """
    Flushes all pending operations to disk
    """
    Trade.session.flush()


def clean_dry_run_db() -> None:
    """
    Remove open_order_id from a dry_run DB
    :return: None
    """
    for trade in Trade.query.filter(Trade.open_order_id.isnot(None)).all():
        if 'dry_run' in trade.open_order_id:
            trade.open_order_id = None


class Trade(_DECL_BASE):
    """
    Class used to define trade structure
    """

    __tablename__ = 'trades'

    id = Column(Integer, primary_key=True)
    exchange = Column(String, nullable=False)
    pair = Column(String, nullable=False, index=True)
    is_open = Column(Boolean, nullable=False, default=True, index=True)
    fee_open = Column(Float, nullable=False, default=0.0)
    fee_close = Column(Float, nullable=False, default=0.0)
    open_rate = Column(Float)
    open_rate_requested = Column(Float)
    close_rate = Column(Float)
    close_rate_requested = Column(Float)
    close_profit = Column(Float)
    stake_amount = Column(Float, nullable=False)
    amount = Column(Float)
    open_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    close_date = Column(Datetime)
    open_order_id = Column(String)
    stop_loss = Column(Float, nullable=False, default=0.0)
    stop_loss_pct = Column(Float, nullable=True)
    initial_stop_loss = Column(Float, nullable=True, default=0.0)
    initial_stop_loss_pct = Column(Float, nullable=True)
    stop_loss_order_id = Column(String, nullable=True, index=True)
    stoploss_last_update = Column(DateTime, nullable=True)
    max_rate = Column(Float, nullable=True, default=0.0)
    min_rate = Column(Float, nullable=True)
    sell_reason = Column(String, nullable=True)
    strategy = Column(String, nullable=True)
    ticker_interval = Column(Integer, nullable=True)

    def __repr__(self):
        open_since = arrow.get(self.open_date).humanize() if self.is_open else 'closed'

        return (f'Trade(id={self.id}, pair={self.pair}, amount={self.amount:.8f}, '
        f'open_rate={self.open_rate:.8f}, open_since={open_since})')


    def to_json(self) -> Dict[str, Any]:
        return {
            
        }
