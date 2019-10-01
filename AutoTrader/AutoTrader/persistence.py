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
