import logging

def setup_logging(level):
    """
    Setup logging.
    Args:
        logfile (str): The path to the logfile to use.
    Returns:
        None
    """
    logging.basicConfig(level=level)
    s_formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    # f_formatter = logging.Formatter('%(levelname)s:%(asctime)s:%(name)s:%(message)s')

    # fh = logging.FileHandler(logfile)
    # fh.setLevel(logging.DEBUG)
    # fh.setFormatter(f_formatter)

    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(s_formatter)

    root_logger = logging.getLogger('')
    root_logger.handlers = []
    # root_logger.addHandler(fh)
    root_logger.addHandler(sh)
