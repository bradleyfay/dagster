.. currentmodule:: dagster

Solids
======

The foundational unit of composition in Dagster.

-----

Simple solids
-------------
.. autodecorator:: lambda_solid

.. autodecorator:: solid

.. autoclass:: SolidDefinition


-------

Inputs & outputs
----------------

.. autoclass:: InputDefinition
   :members:

.. autoclass:: OutputDefinition
   :members:

-------

Composite solids
----------------
.. autodecorator:: composite_solid

.. autoclass:: CompositeSolidDefinition

.. autoclass:: InputMapping

.. autoclass:: OutputMapping

.. autoclass:: ConfigMapping

.. autoclass:: ConfigMappingContext


.. currentmodule:: dagster

Events
------

The objects that can be yielded by the body of solids' compute functions to communicate with the
Dagster framework.

Event types
^^^^^^^^^^^

.. autoclass:: Output
    :members:

.. autoclass:: Materialization
    :members:

.. autoclass:: ExpectationResult
    :members:

.. autoclass:: TypeCheck
    :members:

.. autoclass:: Failure
    :members:

-------

Metadata entries
^^^^^^^^^^^^^^^^

Dagster uses lists of metadata entries to communicate arbitrary user-specified metadata about
structured events.

.. autoclass:: EventMetadataEntry
    :members:

.. autoclass:: JsonMetadataEntryData
    :members:

.. autoclass:: MarkdownMetadataEntryData
    :members:

.. autoclass:: PathMetadataEntryData
    :members:

.. autoclass:: TextMetadataEntryData
    :members:

.. autoclass:: UrlMetadataEntryData
    :members:
