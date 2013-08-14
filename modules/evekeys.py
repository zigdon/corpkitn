import logging
import Queue
import threading
import time

import evelink
import evelink.cache.sqlite
from kitnirc.modular import Module

import schema


_log = logging.getLogger(__name__)


class EveKeysModule(Module):
    """Module to look up details on an EVE API key."""

    def __init__(self, *args, **kwargs):
        super(EveKeysModule, self).__init__(*args, **kwargs)

        self.cache = evelink.cache.sqlite.SqliteCache(".evecache.sqlite3")
        self.results = Queue.Queue()
        self.tasks = Queue.Queue()

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

            _log.info(
                "EveKeys shutting down - %d threads still outstanding." %
                self.tasks.qsize())
            time.sleep(1)

        if not self.tasks.empty():
            _log.warning(
                "EveKeys shutting down - giving up on %d outstanding threads." %
                self.tasks.qsize())

    def _worker():
        while not self.stop:
            try:
                request = self.tasks.get(True, 1)
            except Queue.Empty:
                continue

            _add_key(request)
            self.tasks.task_done()

    def _add_key(request):
        keyid = request['keyid']
        vcode = request['vcode']
        irc_account = request['metadata']['account']

        try:
            api = evelink.api.API(api_key(keyid, vcode), cache=self.cache)
            account = evelink.account.Account(api=api)
            result = account.key_info()
        except evelink.APIError as e:
            _log.warn("Error loading API key(%s): %s" % (keyid, e))
            self.results.put((request, "Failed to load api key %s." % keyid))
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
            return
        except DatabaseError as e:
            _log.warn("Database error saving key(%s): %s" % (keyid, e))
            self.results.put((request, "Database error, try again later."))
            return

        self.results.put((request, summary))

    def _save_key_info(keyid, vcode, irc_account, characters):
        session = schema.Session()

        irc_account = metadata['account']

        # find or create account
        account = session.query(schema.Account).get(irc_account)
        if not account:
            account = schema.Account(irc_account, False)

        # find or create an associated key
        key = session.query(schema.ApiKey).get(keyid)
        if key:
            key.vcode = vcode
        else:
            key = schema.ApiKey(keyid, vcode)
            account.keys.add(key)

        # update/delete existing characters
        for character in key.characters:
            if character.name in characters:
                character.corp = characters[character.name]['corp']
                del(characters[character.name])
            else:
                session.delete(character)

        # add new characters
        for character in characters.itervalues():
            key.characters.add(schema.Character(character['name'],
                                                character['corp']))

        # save everything
        session.add(account)
        session.commit()
        return "%d characters added: %s" % (
            len(characters),
            ", ".join([char['name'] for char in characters]))

    def add_key(self, keyid, vcode, metadata):
        """Look up a given API key, associate with account and characters.

        Args:
            keyid - API key id.
            vcode - API key verification.
            metadata['account'] - IRC account name.
        """
        request = { 'keyid': keyid,
                    'vcode': vcode,
                    'metadata': metadata }
        self.tasks.put(request)


# vim: set ts=4 sts=4 sw=4 et:
