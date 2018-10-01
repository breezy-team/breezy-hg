# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@jelmer.uk>
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
import mercurial.changegroup
import mercurial.manifest
import mercurial.mdiff
import mercurial.node
try:
    from mercurial.cext.parsers import parse_manifest
except ImportError:
    from mercurial.parsers import parse_manifest
import struct

from mercurial.revlog import (
    hash as hghash,
    )

def decode_str(text):
    # Decode according to http://mercurial.selenic.com/wiki/ChangelogEncodingPlan:
    # * attempt to decode with UTF-8, strict
    # * attempt to decode with Latin-1, strict
    # * attempt to decode with UTF-8, replacing unknown characters
    try:
        return text.decode("utf-8", "strict")
    except UnicodeDecodeError:
        pass
    try:
        return text.decode("latin-1", "strict")
    except UnicodeDecodeError:
        pass
    return text.decode("utf-8", "replace")


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
    # An empty username or a username with a "\n" will make the
    # revision text contain two "\n\n" sequences -> corrupt
    # repository since read cannot unpack the revision.
    if not user:
        raise ValueError("empty username")
    if "\n" in user:
        raise ValueError("username %s contains a newline" % repr(user))
    if type(user) != unicode:
        raise TypeError("user should be unicode string")
    if type(desc) != unicode:
        raise TypeError("desc should be unicode string")

    user = user.encode("utf-8")
    desc = desc.encode("utf-8")

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
    text = str(text)
    last = text.index("\n\n")
    desc = decode_str(text[last + 2:])
    l = text[:last].split('\n')
    manifest = mercurial.node.bin(l[0])
    user = decode_str(l[1])

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


def pack_chunk_iter(entries, textbase):
    """Create a chained series of Mercurial deltas.

    The first entry is not packed but rather used as a base for the delta
    for the second.

    :param entries: Iterator over (fulltext, (p1, p2), link) tuples.
    :return: iterator over delta chunks
    """
    for (fulltext, (p1, p2), link) in entries:
        assert len(p1) == 20
        assert len(p2) == 20
        node = hghash(fulltext, p1, p2)
        assert len(node) == 20
        assert len(link) == 20
        delta = mercurial.mdiff.bdiff.bdiff(textbase, fulltext)
        chunk = struct.pack("20s20s20s20s", node, p1, p2, link) + delta
        yield chunk
        textbase = fulltext


def unpack_chunk_iter(chunk_iter, lookup_base):
    """Unpack a series of Mercurial deltas.

    :param chunk_iter: Iterator over chunks to unpack
    :param lookup_base: Function to look up contents of bases for deltas.
    :return: Iterator over (fulltext, node, (p1, p2), link) tuples.
    """
    fulltext_cache = {}
    base = None
    for chunk in chunk_iter:
        node, p1, p2, link = struct.unpack("20s20s20s20s", chunk[:80])
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
        fulltext = mercurial.mdiff.patch(textbase, delta)
        yield fulltext, node, (p1, p2), link
        fulltext_cache[node] = fulltext
        base = node


def parse_manifest(fulltext):
    """Parse a manifest.

    :param fulltext: Text to parse
    :return: Tuple with the manifest and flags dictionary
    """
    manifest = mercurial.manifest.manifestdict()
    flags = {}
    parse_manifest(manifest, flags, fulltext)
    return manifest, flags


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


def serialize_file_text(meta, text):
    if meta or text.startswith('\1\n'):
        mt = ["%s: %s\n" % (k, v) for k, v in sorted(meta.iteritems())]
        text = "\1\n%s\1\n%s" % ("".join(mt), text)
    return text


def deserialize_file_text(text):
    if not text.startswith("\1\n"):
        return ({}, text)
    s = text.index('\1\n', 2)
    mt = text[2:s]
    meta = {}
    for l in mt.splitlines():
        k, v = l.split(": ", 1)
        meta[k] = v
    return meta, text[s+2:]


def chunkiter(source, progress=None):
    """iterate through the chunks in source, yielding a sequence of chunks
    (strings)"""
    while 1:
        c = mercurial.changegroup.getchunk(source)
        if not c:
            break
        elif progress is not None:
            progress()
        yield c
