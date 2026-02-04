import re

def get_query_params(query, designator = "&"):
    """Extracts all unbound parameters from a SQL query identified by a designator character.

    Args:
        query (str): a SQL query, as a string (usually read from a file).
        designator (str, optional): the character that denotes an unbound parameter in the query. Defaults to "&".

    Returns:
        list: the names of all unbound parameters in the query, including designator character.
    """
    pattern = designator + r"\w+" # matches all Unicode word characters (alphanumeric + underscores) preceded by the designator character.
    params = re.findall(pattern, query)
    return params

def build_query_params_dict(param_names, param_values):
    """Compiles a list of unbound parameter names and a corresponding list of values into a dictionary.

    Args:
        param_names (list): strings of the format (designator character)ParamName.
        param_values (list): the values to be assigned to their corresponding parameter name.

    Returns:
        dict: a dictionary with unbound parameter names as keys and their desired values as values.
    """
    return dict(zip(param_names, param_values))

def define_query_params(query, params_dict):
    """Replaces unbound parameters in a SQL query with their desired values.

    Args:
        query (str): a SQL query containing unbound parameters.
        params_dict (dict[str : str]): a dictionary of parameter names and values, as produced by build_query_params_dict().
            Or, you can provide it yourself, if you're freaky.

    Returns:
        str: query with parameter placeholders replaced with actual values.
    """
    new_query = query

    for name, value in params_dict.items():
        # This wraps parameter values in single quotes for compatibility with Oracle DBs.
        # May be problematic for numeric input but most of our parameters are strings so
        # I'm going to leave it like this until it becomes a problem.
        new_query = re.sub(name, "'" + value + "'", new_query)
    
    return new_query

def preprocess_query(query, params = []):
    """Binds all parameters in a SQL query using user-provided inputs.

    Args:
        query (str): a SQL query containing unbound parameters.
        params (list[str]):

    Returns:
        str: the same query with parameters bound to provided values.
    """
    param_names = get_query_params(query)
    param_values = []

    if params:
        if len(params) == len(param_names):
            param_values = params
        elif len(params) < len(param_names):
            raise ValueError('Not enough parameter values provided.')
        else:
            raise ValueError('Too many parameter values provided.')
    else: 
        for param in param_names:
            response = input("Enter value for parameter {0}: ".format(param))
            param_values.append(response)

    param_dict = build_query_params_dict(param_names, param_values)
    return define_query_params(query, param_dict)

def get_query_from_file(path, params = []):
    """Loads and preprocesses query text from a .sql file containing a single query.
    Convenience wrapper for the other methods in this package.

    Args:
        path (path): path to .sql file containing a single query.
        params (list[str]): list of parameter values to be inserted into query.
    
        Returns:
            str: fully preprocessed query string, ready to be used for ODBC operations. Probably.
    """
    with open(path) as sql_file:
        original_query = sql_file.read()
        new_query = preprocess_query(original_query, params)
        return new_query
