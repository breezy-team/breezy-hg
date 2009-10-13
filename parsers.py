# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

# parse_changeset is based on mercurial.changelog.changelog.read:
#
# Copyright 2005-2007 Matt Mackall <mpm@selenic.com>
# Published under the GNU GPLv2 or later

"""Parsers etc.

This code should probably be submitted to upstream Mercurial for inclusion.
"""

import mercurial.changelog
import mercurial.encoding
import mercurial.node
import struct

from mercurial.revlog import (
    hash as hghash,
    )


def format_changeset(manifest, files, user, date, desc, extra):
    """Serialize a Mercurial changeset.

    :param manifest: Manifest ID for this changeset, as 20-byte string
    :param files: Array of the files modified by this changeset
    :param user: Name + email of the committer
    :param date: Date of the commit
    :param desc: Commit message
    :param extra: Dictionary with extra revision properties
    :return: String with formatted revision
    """
    user = user.strip()
    # An empty username or a username with a "\n" will make the
    # revision text contain two "\n\n" sequences -> corrupt
    # repository since read cannot unpack the revision.
    if not user:
        raise ValueError("empty username")
    if "\n" in user:
        raise ValueError("username %s contains a newline" % repr(user))

    # strip trailing whitespace and leading and trailing empty lines
    desc = '\n'.join([l.rstrip() for l in desc.splitlines()]).strip('\n')

    user = mercurial.encoding.fromlocal(user)
    desc = mercurial.encoding.fromlocal(desc)

    if not isinstance(date, tuple):
        raise TypeError("date is not a tuple")
    parseddate = "%d %d" % date
    if extra and extra.get("branch") in ("default", ""):
        del extra["branch"]
    if extra:
        extra = mercurial.changelog.encodeextra(extra)
        parseddate = "%s %s" % (parseddate, extra)
    l = [mercurial.node.hex(manifest), user, parseddate] + \
        sorted(files) + ["", desc]
    return "\n".join(l)


def parse_changeset(text):
    """Parse a Mercurial changeset.
    
    :param text: Text to parse
    :return: Tuple with (manifest, user, (time, timezone), files, desc, extra)
    """
    last = text.index("\n\n")
    desc = mercurial.encoding.tolocal(text[last + 2:])
    l = text[:last].split('\n')
    manifest = mercurial.node.bin(l[0])
    user = mercurial.encoding.tolocal(l[1])

    extra_data = l[2].split(' ', 2)
    if len(extra_data) != 3:
        time = float(extra_data.pop(0))
        try:
            # various tools did silly things with the time zone field.
            timezone = int(extra_data[0])
        except:
            timezone = 0
        extra = {}
    else:
        time, timezone, extra = extra_data
        time, timezone = float(time), int(timezone)
        extra = mercurial.changelog.decodeextra(extra)
    files = l[3:]
    return (manifest, user, (time, timezone), files, desc, extra)


def pack_chunk_iter(entries):
    """Create a chained series of Mercurial deltas.

    :param entries: Iterator over (fulltext, (p1, p2)) tuples.
    :return: iterator over delta chunks
    """
    # TODO: Let caller pass in an actual valid parent text.
    cs = mercurial.node.nullid
    textbase = ""
    for (fulltext, (p1, p2)) in entries:
        assert len(p1) == 20
        assert len(p2) == 20
        node = hghash(fulltext, p1, p2)
        assert len(node) == 20
        chunk = struct.pack("20s20s20s20s", node, p1, p2, cs) +\
                mercurial.mdiff.bdiff.bdiff(textbase, fulltext)
        yield chunk
        cs = node
        textbase = fulltext


def unpack_chunk_iter(chunk_iter, lookup_base):
    """Unpack a series of Mercurial deltas.

    :param chunk_iter: Iterator over chunks to unpack
    :param lookup_base: Function to look up contents of 
        bases for deltas.
    :return: Iterator over (fulltext, node, (p1, p2)) tuples.
    """
    fulltext_cache = {}
    base = None
    for chunk in chunk_iter:
        node, p1, p2, cs = struct.unpack("20s20s20s20s", chunk[:80])
        if base is None:
            base = p1
        delta = buffer(chunk, 80)
        del chunk
        if base == mercurial.node.nullid:
            textbase = ""
        else:
            try:
                textbase = fulltext_cache[base]
            except KeyError:
                textbase = lookup_base(base)
        fulltext = mercurial.mdiff.patches(textbase, [delta])
        yield fulltext, node, (p1, p2)
        fulltext_cache[node] = fulltext
        base = node


def parse_manifest(fulltext):
    """Parse a manifest.

    :param fulltext: Text to parse
    :return: Tuple with the manifest and flags dictionary
    """
    manifest = mercurial.manifest.manifestdict()
    flags = {}
    mercurial.parsers.parse_manifest(manifest, flags, fulltext)
    return manifest, flags


def unpack_manifest_chunks(chunkiter, lookup_base):
    """Unpack manifests in a stream of deltas.

    :param chunkiter: Iterator over delta chunks
    :param lookup_base: Callback for looking up an arbitrary manifest text.

    Yields tuples with key, parents, manifest and flags dictionary
    """
    for (fulltext, hgkey, hgparents) in unpack_chunk_iter(chunkiter, 
                                                          lookup_base):
        (manifest, flags) = parse_manifest(fulltext)
        yield hgkey, hgparents, manifest, flags


def format_manifest(manifest, flags):
    lines = []
    for path in sorted(manifest.keys()):
        node = manifest[path]
        assert type(path) is str
        assert type(node) is str and len(node) in (20, 40)
        if len(node) == 20:
            node = mercurial.node.hex(node)
        lines.append("%s\0%s%s\n" % (path, node, flags.get(path, "")))
    return "".join(lines)
