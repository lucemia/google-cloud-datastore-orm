

__all__ = ['AbstractAdapter',
           'BaseConfiguration',
           'BaseConnection',
           'ConfigOption',
           'Configuration',
           'Connection',
           'IdentityAdapter',
           'MultiRpc',
           'TransactionalConnection',
           'TransactionOptions',
          ]

import collections
import copy
import functools
import logging
import googledatastore as datastore


_MAX_ID_BATCH_SIZE = 1000 * 1000 * 1000

def _positional(max_pos_args):
  """A decorator to declare that only the first N arguments may be positional.

  Note that for methods, n includes 'self'.
  """
  def positional_decorator(wrapped):
    @functools.wraps(wrapped)
    def positional_wrapper(*args, **kwds):
      if len(args) > max_pos_args:
        plural_s = ''
        if max_pos_args != 1:
          plural_s = 's'
        raise TypeError(
          '%s() takes at most %d positional argument%s (%d given)' %
          (wrapped.__name__, max_pos_args, plural_s, len(args)))
      return wrapped(*args, **kwds)
    return positional_wrapper
  return positional_decorator

class Configuration(object):

  STRONG_CONSISTENCY = 0
  """A read consistency that will return up to date results."""

  EVENTUAL_CONSISTENCY = 1
  """A read consistency that allows requests to return possibly stale results.

  This read_policy tends to be faster and less prone to unavailability/timeouts.
  May return transactionally inconsistent results in rare cases.
  """

  APPLY_ALL_JOBS_CONSISTENCY = 2
  """A read consistency that aggressively tries to find write jobs to apply.

  Use of this read policy is strongly discouraged.

  This read_policy tends to be more costly and is only useful in a few specific
  cases. It is equivalent to splitting a request by entity group and wrapping
  each batch in a separate transaction. Cannot be used with non-ancestor
  queries.
  """



class ConfigOption(object):
  """A descriptor for a Configuration option.

  This class is used to create a configuration option on a class that inherits
  from BaseConfiguration. A validator function decorated with this class will
  be converted to a read-only descriptor and BaseConfiguration will implement
  constructor and merging logic for that configuration option. A validator
  function takes a single non-None value to validate and either throws
  an exception or returns that value (or an equivalent value). A validator is
  called once at construction time, but only if a non-None value for the
  configuration option is specified the constructor's keyword arguments.
  """

  def __init__(self, validator):
    self.validator = validator

  def __get__(self, obj, objtype):
    if obj is None:
      return self
    return obj._values.get(self.validator.__name__, None)

  def __set__(self, obj, value):
    raise AttributeError('Configuration options are immutable (%s)' %
                         (self.validator.__name__,))

  def __call__(self, *args):
    """Gets the first non-None value for this option from the given args.

    Args:
      *arg: Any number of configuration objects or None values.

    Returns:
      The first value for this ConfigOption found in the given configuration
    objects or None.

    Raises:
      datastore_errors.BadArgumentError if a given in object is not a
    configuration object.
    """
    name = self.validator.__name__
    for config in args:

      if isinstance(config, (type(None), apiproxy_stub_map.UserRPC)):
        pass
      elif not isinstance(config, BaseConfiguration):
        raise datastore_errors.BadArgumentError(
            'invalid config argument (%r)' % (config,))
      elif name in config._values and self is config._options[name]:
        return config._values[name]
    return None


class TransactionOptions(Configuration):
  """An immutable class that contains options for a transaction."""

  NESTED = 1
  """Create a nested transaction under an existing one."""

  MANDATORY = 2
  """Always propagate an existing transaction, throw an exception if there is
  no existing transaction."""

  ALLOWED = 3
  """If there is an existing transaction propagate it."""

  INDEPENDENT = 4
  """Always use a new transaction, pausing any existing transactions."""

  _PROPAGATION = frozenset((NESTED, MANDATORY, ALLOWED, INDEPENDENT))

  @ConfigOption
  def propagation(value):
    """How existing transactions should be handled.

    One of NESTED, MANDATORY, ALLOWED, INDEPENDENT. The interpertation of
    these types is up to higher level run-in-transaction implementations.

    WARNING: Using anything other than NESTED for the propagation flag
    can have strange consequences.  When using ALLOWED or MANDATORY, if
    an exception is raised, the transaction is likely not safe to
    commit.  When using INDEPENDENT it is not generally safe to return
    values read to the caller (as they were not read in the caller's
    transaction).

    Raises: datastore_errors.BadArgumentError if value is not reconized.
    """
    if value not in TransactionOptions._PROPAGATION:
      raise datastore_errors.BadArgumentError('Unknown propagation value (%r)' %
                                              (value,))
    return value

  @ConfigOption
  def xg(value):
    """Whether to allow cross-group transactions.

    Raises: datastore_errors.BadArgumentError if value is not a bool.
    """
    if not isinstance(value, bool):
      raise datastore_errors.BadArgumentError(
          'xg argument should be bool (%r)' % (value,))
    return value

  @ConfigOption
  def retries(value):
    """How many retries to attempt on the transaction.

    The exact retry logic is implemented in higher level run-in-transaction
    implementations.

    Raises: datastore_errors.BadArgumentError if value is not an integer or
      is not greater than zero.
    """
    datastore_types.ValidateInteger(value,
                                    'retries',
                                    datastore_errors.BadArgumentError,
                                    zero_ok=True)
    return value

  @ConfigOption
  def app(value):
    """The application in which to perform the transaction.

    Raises: datastore_errors.BadArgumentError if value is not a string
      or is the empty string.
    """
    datastore_types.ValidateString(value,
                                   'app',
                                   datastore_errors.BadArgumentError)
    return value

class AbstractAdapter(object):
  pass


class IdentityAdapter(AbstractAdapter):
  pass


class FakeAsync(object):
  def __init__(self, value):
    self.value = value

  def get_result(self):
    return self.value

class BaseConnection(object):

  @_positional(1)
  def __init__(self, adapter=None, config=None, _api_version='_DATASTORE_V3'):
    self.__adapter = adapter

  @property
  def adapter(self):
    return self.__adapter

  @property
  def config(self):
    return self.__config

  def async_get(self, config, keys, local_extra_hook):

    import patch
    from google.appengine.datastore import entity_pb

    converter = patch.Converter()


    req = datastore.LookupRequest()
    for key in keys:
      key_pb = datastore.Key()
      path_pb = key_pb.path_element.add()
      path_pb.kind = key.kind()
      if key.name():
        path_pb.name = key.name()
      else:
        path_pb.id = key.id()
      req.key.extend([key_pb])

    resp = datastore.lookup(req)
    if not resp.found:
      raise
    else:
      entity = entity_pb.EntityProto()
      # import pdb; pdb.set_trace()
      converter.v1_to_v3_entity(resp.found[0].entity, entity)
      entity.set_kind(key.kind())
      entity = self.adapter.pb_to_entity(entity)

      return local_extra_hook([FakeAsync(entity)])

class Connection(BaseConnection):
  pass



def _ToDatastoreError(err):
  """Converts an apiproxy.ApplicationError to an error in datastore_errors.

  Args:
    err: An apiproxy.ApplicationError object.

  Returns:
    An instance of a subclass of datastore_errors.Error.
  """
  return _DatastoreExceptionFromErrorCodeAndDetail(err.application_error,
                                                   err.error_detail)


def _DatastoreExceptionFromErrorCodeAndDetail(error, detail):
  """Converts a datastore_pb.Error into a datastore_errors.Error.

  Args:
    error: A member of the datastore_pb.Error enumeration.
    detail: A string providing extra details about the error.

  Returns:
    An instance of a subclass of datastore_errors.Error.
  """
  exception_class = {
      datastore_pb.Error.BAD_REQUEST: datastore_errors.BadRequestError,
      datastore_pb.Error.CONCURRENT_TRANSACTION:
          datastore_errors.TransactionFailedError,
      datastore_pb.Error.INTERNAL_ERROR: datastore_errors.InternalError,
      datastore_pb.Error.NEED_INDEX: datastore_errors.NeedIndexError,
      datastore_pb.Error.TIMEOUT: datastore_errors.Timeout,
      datastore_pb.Error.BIGTABLE_ERROR: datastore_errors.Timeout,
      datastore_pb.Error.COMMITTED_BUT_STILL_APPLYING:
          datastore_errors.CommittedButStillApplying,
      datastore_pb.Error.CAPABILITY_DISABLED:
          apiproxy_errors.CapabilityDisabledError,
  }.get(error, datastore_errors.Error)

  if detail is None:
    return exception_class()
  else:
    return exception_class(detail)
