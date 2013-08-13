from Queue import queue, Empty

import logging
import threading
import time

import evelink
import evelink.cache.sqlite

from kitnirc.modular import Module

import schema


_log = logging.getLogger(__name__)


class EveKeyError(Exception):
    pass


class EveKeysModule(Module):
    """Module to look up details on an EVE API key."""

    def __init__(self, *args, **kwargs):
        super(EveKeysModule, self).__init__(*args, **kwargs)

        self.cache = evelink.cache.sqlite.SqliteCache(".evecache.sqlite3")
        self.results = queue()
        self.tasks = queue()

        for i in range(5):
            t = threading.Thread(target=self._worker)
            t.daemon = True
            t.start()

        self._stop = False

    def start(self, *args, **kwargs):
        super(EveKeysModule, self).start(*args, **kwargs)
        self._stop = False

    def stop(self, *args, **kwargs):
        super(EveKeysModule, self).stop(*args, **kwargs)
        self.stop = True
        for _ in range(10):
            if self.tasks.empty():
                break

            _log.info("Evekeys still has %d threads outstanding." %
                       self.tasks.qsize())
            time.sleep(1)

        if not self.tasks.empty():
            _log.warning("Evekeys giving up with %d threads outstanding." %
                          self.tasks.qsize())

    def _worker():
        while not self.stop:
            try:
                request = self.tasks.get(True, 5)
            except Empty:
                continue

            _add_key(request)
            self.tasks.task_done()

    def _add_key(request):
        keyid = request['keyid']
        vcode = request['vcode']
        irc_account = request['metadata']['account'].lower()

        try:
            api = evelink.api.API(api_key(keyid, vcode), cache=self.cache)
            account = evelink.account.Account(api=api)
            result = account.key_info()
        except evelink.APIError as e:
            _log.warn("Error loading API key(%s): %s" % (keyid, e))
            self.results.put((request, "Failed to load api key."))
            return

        if result:
            _log.debug("key: %s, characters: %s" % (keyid,
                ", ".join(char['name'] for char
                          in result['characters'].itervalues())))
        else:
            _log.warn("key: %s, invalid key.")
            self.results.put((request, "Invalid key."))
            return

        try:
            summary = _save_key_info(keyid,
                                     vcode,
                                     irc_account,
                                     result['characters'])
            self.results.put((request, summary))
            return

        except DatabaseError as e:
            _log.warn("Database error saving key(%s): %s" % (keyid, e))
            self.results.put((request, "Database error, try again later."))
            return

    def _save_key_info(keyid, vcode, irc_account, characters):
        session = schema.Session()

        irc_account = metadata['account'].lower()
        account, _ = schema.find_or_create(session,
                                           schema.Account,
                                           account=irc_account)

        # add key to the account
        key, _ = schema.find_or_create(session,
                                       schema.ApiKey,
                                       keyid=keyid,
                                       vcode=vcode)

        # add characters
        for character in characters.itervalues():
            data = { 'name': character['name'],
                     'corp': character['corp']['name'] }
            char, _ = schema.find_or_create(session,
                                            schema.Character,
                                            data=data,
                                            name=character['name'])

        session.commit()
        return "%d characters added: %s" % (
            len(characters),
            ", ".join([char['name'] for char in characters]))

    def add_key(self, **kwargs):
        """Look up a given API key, associate with account and characters.

        Args:
            keyid - API key id.
            vcode - API key verification.
            metadata['account'] - IRC account name.
        """
        self.tasks.put(kwargs)


# vim: set ts=4 sts=4 sw=4 et:
