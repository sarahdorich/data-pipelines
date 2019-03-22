#!/usr/bin/python

"""Extract features from GA data fields

Contains methods for extracting features from Google Analytics data fields.

"""


def get_page_path_levels(page_path):
    """ Get levels of a page path

    Args:
        page_path (str): page path

    Returns:
        page_path_levels_list (list of str): page path levels

    """
    if page_path == '(entrance)':
        page_path = '/entrance'
    separator = '/'
    if page_path[0] == separator:
        page_path = page_path[1:]
    page_path_levels_list = page_path.split(separator)
    return page_path_levels_list


def extract_page_path_level_n(page_path, n):
    """Extract the nth level of a page path

    Args:
        page_path (str or list of strings):
        n (int):

    Returns:
        page_path_level_n (str): nth level of the page path

    """
    n_adj = n - 1  # adjust n for Python indexing (starts at 0)
    if isinstance(page_path, str):
        page_path_levels_list = get_page_path_levels(page_path)
    else:
        page_path_levels_list = page_path
    if len(page_path_levels_list) > n_adj:
        page_path_level_n = page_path_levels_list[n_adj]
    else:
        page_path_level_n = None
    return page_path_level_n


def extract_source_medium(source_medium_val, separator=' / '):
    """Extract the source and the medium from the ga:sourceMedium dimension

    Args:
        source_medium_val (str): value from the ga:sourceMedium dimension
        separator (str): value Google uses for separating the source and the medium

    Returns:
        source (str): source
        medium (str): medium

    """
    source_medium_list = source_medium_val.split(separator)
    if len(source_medium_list) > 1:
        source = source_medium_list[0]
        medium = source_medium_list[1]
    else:
        source = None
        medium = None
    return source, medium







