#!/usr/bin/python

""" Helper methods that can be performed on Python lists
"""


def is_in(element, collection: iter):
    """ Check if element is in collection

    Args:
        element: (object)
        collection: (list)

    Returns:
        out: (bool) indicates whether or not element is in collection

    Example:
        my_list = ['a', 'b', 4, 23, 6, 'c', 35]
        is_element_in_list = is_in('c', my_list)
    """
    out = element in collection
    return out
