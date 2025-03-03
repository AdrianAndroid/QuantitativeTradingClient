def stock_history_filepath(_code, _name, _type):
    history = f'{HISTORY_PATH}/{_type}{_code}.json'
    return history
