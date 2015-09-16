#!/usr/bin/env python
# -*- coding: utf-8 -*-
## mwu_downloader.py
## A helpful tool to fetch data from website & generate mdx source file
##
## This program is a free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation, version 3 of the License.
##
## You can get a copy of GNU General Public License along this program
## But you can always get it from http://www.gnu.org/licenses/gpl.txt
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
## GNU General Public License for more details.
import os
import re
import fileinput
import urllib
import requests
from os import path
from datetime import datetime
from multiprocessing import Pool
from collections import OrderedDict


MAX_PROCESS = 1
BUF_SIZE = 6000


def fullpath(file, suffix='', base_dir=''):
    if base_dir:
        return ''.join([os.getcwd(), path.sep, base_dir, file, suffix])
    else:
        return ''.join([os.getcwd(), path.sep, file, suffix])


def readdata(file, base_dir=''):
    fp = fullpath(file, base_dir=base_dir)
    if not path.exists(fp):
        print("%s was not found under the same dir of this tool." % file)
    else:
        fr = open(fp, 'rU')
        try:
            return fr.read()
        finally:
            fr.close()
    return None


def dump(data, file, mod='w'):
    fname = fullpath(file)
    fw = open(fname, mod)
    try:
        fw.write(data)
    finally:
        fw.close()


def removefile(file):
    if path.exists(file):
        os.remove(file)


def info(l, s='word'):
    return '%d %ss' % (l, s) if l>1 else '%d %s' % (l, s)


def getpage(session, link, base_url):
    url = ''.join([base_url, link])
    if session:
        r = session.get(url, timeout=10, allow_redirects=False)
    else:
        r = requests.get(url, timeout=10, allow_redirects=False)
    if r.status_code == 200:
        return r.content
    else:
        return None


def getwordlist(file, base_dir='', toint=False):
    words = readdata(file, base_dir)
    if words:
        wordlist = []
        p = re.compile(r'\s*\n\s*')
        words = p.sub('\n', words).strip()
        for word in words.split('\n'):
            try:
                w, u = word.split('\t')
                if toint:
                    wordlist.append((int(w.strip()), int(u.strip())))
                else:
                    wordlist.append((w, u))
            except Exception, e:
                import traceback
                print traceback.print_exc()
                print word
        return wordlist
    print("%s: No such file or file content is empty." % file)
    return []


class downloader:
#common logic
    def __init__(self, name):
        self.DIC_T = name

    @property
    def diff(self):
        pass

    @property
    def base_url(self):
        pass

    @property
    def parts(self):
        pass

    def postdata(self, start, show):
        pass

    def makeword(self, page, words):
        pass

    def strip_key(self, word):
        pass

    def set_repcls(self):
        pass

    def make_entry(self, cn, v):
        pass

    def make_pron(self, appendix):
        pass

    def format_entry(self, key, line, crefs, links, appendix):
        pass

    def load_appendix(self, dir):
        pass

    def dump_appendix(self, dir, fm, appendix):
        pass

    def cleansp(self, html):
        p = re.compile(r'\s+')
        html = p.sub(' ', html)
        p = re.compile(r'<!--[^<>]+?-->')
        html = p.sub('', html)
        p = re.compile(r'\s*<br/?>\s*')
        html = p.sub('<br>', html)
        p = re.compile(r'(\s*<br>\s*)*(<hr[^>]*>)(\s*<br>\s*)*', re.I)
        html = p.sub(r'\2', html)
        p = re.compile(r'(\s*<br>\s*)*(<(?:/?(?:div|p)[^>]*|br)>)(\s*<br>\s*)*', re.I)
        html = p.sub(r'\2', html)
        p = re.compile(r'\s*(<(?:/?(?:div|p|ul|li)[^>]*|br)>)\s*', re.I)
        html = p.sub(r'\1', html)
        p = re.compile(r'\s+(?=[,\.;\?\!])')
        html = p.sub(r'', html)
        p = re.compile(r'\s+(?=</?\w+>[\)\]\s])')
        html = p.sub(r'', html)
        return html

    def __mod(self, flag):
        return 'a' if flag else 'w'

    def __sort_entries(self, file, sdir):
        if path.exists(file):
            refs = getwordlist('cref.txt', sdir)
            lns, entries = [], []
            for ln in fileinput.input(file):
                ln = ln.strip()
                if ln == '</>':
                    entries.append((lns[0], lns[1]))
                    del lns[:]
                elif ln:
                    lns.append(ln)
            words, j = OrderedDict(), 0
            for no, word in refs:
                st, ps = no.split('-')
                key = int(st)+int(ps)
                words[key] = entries[j]
                j += 1
            fw = open(file, 'w')
            try:
                for k, v in sorted(words.iteritems(), key=lambda d: d[0]):
                    fw.write('\n'.join([v[0], v[1], '</>\n']))
            finally:
                fw.close()

    def __dumpwords(self, sdir, words, sfx='', finished=True):
        f = fullpath('rawhtml.txt', sfx, sdir)
        if len(words):
            mod = self.__mod(sfx)
            fw = open(f, mod)
            try:
                [fw.write('\n'.join([en[0], en[1], '</>\n'])) for en in words]
            finally:
                fw.close()
        elif not path.exists(f):
            fw = open(f, 'w')
            fw.write('\n')
            fw.close()
        if sfx and finished:
            removefile(fullpath('failed.txt', '', sdir))
            l = -len(sfx)
            cmd = '\1'
            nf = f[:l]
            if path.exists(nf):
                msg = "Found rawhtml.txt in the same dir, delete?(default=y/n)"
                cmd = 'y'#raw_input(msg)
            if cmd == 'n':
                return
            elif cmd != '\1':
                removefile(nf)
            os.rename(f, nf)
        self.__sort_entries(fullpath('rawhtml.txt', base_dir=sdir), sdir)

    def __fetchdata_and_make_mdx(self, arg, failed=None, suffix=''):
        sdir = arg['dir']
        if failed:
            pl, failed = failed, []
        else:
            part, pl, failed = arg['alp'], [], []
            bg, ed = part[0], part[1]
            times = int((ed-bg)/30)
            last = ed - bg - times*30
            for i in xrange(0, times):
                for j in xrange(0, 30):
                    pl.append((i*30+bg, j))
            for j in xrange(0, last):
                pl.append(((i+1)*30+bg, j))
        words, crefs, logs, count= [], OrderedDict(), [], 1
        leni = len(pl)
        while leni:
            for start, pos in pl:
                if count % 10 == 0:
                    print ".",
                    if count % 300 == 0:
                        print count,
                try:
                    print '-',
                    page = self.postdata(start, pos)
                    print '+',
                    if page:
                        word = self.makeword(page, words)
                        if word:
                            crefs[''.join([str(start), '-', str(pos)])] = word
                            count += 1
                        else:
                            failed.append((start, pos))
                    else:
                        failed.append((start, pos))
                except Exception, e:
                    import traceback
                    print traceback.print_exc()
                    print "'%d: %d' failed, retry automatically later" % (start, pos)
                    failed.append((start, pos))
            lenr = len(failed)
            if lenr >= leni:
                break
            else:
                leni = lenr
                pl, failed = failed, []
        print "%s browsed" % info(count-1),
        if crefs:
            mod = self.__mod(path.exists(fullpath('cref.txt', base_dir=sdir)))
            dump(''.join(['\n'.join(['\t'.join([k, v]) for k, v in crefs.iteritems()]), '\n']), ''.join([sdir, 'cref.txt']), mod)
        if failed:
            dump(''.join(['\n'.join(['\t'.join([str(s), str(p)]) for s, p in failed]), '\n']), ''.join([sdir, 'failed.txt']))
            self.__dumpwords(sdir, words, '.part', False)
        else:
            print ", 0 word failed"
            self.__dumpwords(sdir, words, suffix)
        if logs:
            mod = self.__mod(path.exists(fullpath('log.txt', base_dir=sdir)))
            dump('\n'.join(logs), ''.join([sdir, 'log.txt']), mod)

    def start(self, arg):
        import socket
        socket.setdefaulttimeout(120)
        import sys
        reload(sys)
        sys.setdefaultencoding('utf-8')
        sdir = arg['dir']
        fp1 = fullpath('rawhtml.txt.part', base_dir=sdir)
        fp2 = fullpath('failed.txt', base_dir=sdir)
        fp3 = fullpath('rawhtml.txt', base_dir=sdir)
        if path.exists(fp1) and path.exists(fp2):
            print ("Continue last failed")
            failed = getwordlist('failed.txt', sdir, True)
            self.__fetchdata_and_make_mdx(arg, failed, '.part')
        elif not path.exists(fp3):
            print ("New session started")
            self.__fetchdata_and_make_mdx(arg)

    def __dump_buf(self, fw, words, cn, buf, clear=False):
        if clear or len(buf)>=BUF_SIZE:
            end = len(buf) if clear else -BUF_SIZE/2
            lns = []
            for k, v in buf.items()[:end]:
                lns.append('\n'.join([k, self.make_entry(cn, v), '</>']))
                words.append(k)
            fw.write(''.join(['\n'.join(lns), '\n']))
            if not clear:
                tbuf = OrderedDict(buf.items()[-BUF_SIZE/2:])
                buf.clear()
                buf = tbuf
        return buf

    def combinefiles(self, dir):
        print "combining files..."
        times = 0
        for d in os.listdir(fullpath(dir)):
            if path.isdir(fullpath(''.join([dir, d, path.sep]))):
                times += 1
        for fn in ['cref.txt', 'log.txt']:
            fw = open(fullpath(''.join([dir, fn])), 'w')
            for i in xrange(1, times+1):
                sdir = ''.join([dir, '%d'%i, path.sep])
                if path.exists(fullpath(fn, base_dir=sdir)):
                    fw.write('\n'.join([readdata(fn, sdir).strip(), '']))
            fw.close()
        buf, words, crefs, links, logs = OrderedDict(), [], OrderedDict(), OrderedDict(), []
        refs = getwordlist('cref.txt', dir)
        print "%s totally." % info(len(refs), 'raw item')
        for k, v in refs:
            v = self.strip_key(v)
            crefs[v.lower()] = v
        if path.exists(fullpath(''.join([dir, 'links.txt']))):
            for k, v in getwordlist('links.txt', dir):
                crefs[k.strip().lower()] = v
        appendix = self.load_appendix(dir)
        self.set_repcls()
        cn = 'MWT' if self.diff=='t' else self.DIC_T
        fm = ''.join([dir, cn, path.extsep, 'txt'])
        fw = open(fullpath(fm), 'w')
        try:
            for i in xrange(1, times+1):
                sdir = ''.join([dir, '%d'%i, path.sep])
                file = fullpath('rawhtml.txt', base_dir=sdir)
                lns = []
                for ln in fileinput.input(file):
                    ln = ln.strip()
                    if ln == '</>':
                        p = re.compile(r'<sup[^<>]*>\s*(\d+)\s*</sup>')
                        m = p.search(lns[0])
                        key = self.strip_key(lns[0])
                        sk = m.group(1) if m else ''
                        if len(buf) == 0:
                            buf[key] = [(sk, self.format_entry(key, lns[1], crefs, links, appendix))]
                        elif key in buf:
                            buf[key].append((sk, self.format_entry(key, lns[1], crefs, links, appendix)))
                        else:
                            buf = self.__dump_buf(fw, words, cn, buf)
                            buf[key] = [(sk, self.format_entry(key, lns[1], crefs, links, appendix))]
                        del lns[:]
                    elif ln:
                        lns.append(ln)
            if buf:
                self.__dump_buf(fw, words, cn, buf, True)
        finally:
            fw.close()
        if self.diff != 't':
            dump(self.make_pron(appendix), fm, 'a')
        print "%s " % info(len(words)),
        dump('\n'.join(words), ''.join([dir, 'words.txt']), 'w')
        self.dump_appendix(dir, fm, appendix)
        entries = OrderedDict([(w.lower(), None) for w in words])
        lns = []
        for link, ref in links.iteritems():
            if not link.lower() in entries:
                lns.append('\n'.join([link, '@@@LINK=%s'%ref, '</>\n']))
        if lns:
            print "%s " % info(len(lns), 'link'),
            dump(''.join(['\n'.join(['\t'.join([k, v]) for k, v in links.iteritems()]), '\n']), ''.join([dir, 'links.txt']))
            dump(''.join(lns), fm, 'a')
        print "totally."
        if logs:
            mod = self.__mod(path.exists(fullpath('log.txt', base_dir=dir)))
            dump('\n'.join(logs), ''.join([dir, 'log.txt']), mod)


def f_start((obj, arg)):
    obj.start(arg)


def multiprocess_fetcher(obj, dir):
    pl = obj.parts
    times = len(pl)
    if not path.exists(dir):
        os.mkdir(dir)
    for i in xrange(1, times+1):
        subdir = ''.join([dir, '%d'%i])
        subpath = fullpath(subdir)
        if not path.exists(subpath):
            os.mkdir(subpath)
    pool = Pool(MAX_PROCESS)
    leni = times+1
    while 1:
        args = []
        for i in xrange(1, times+1):
            sdir = ''.join([dir, '%d'%i, path.sep])
            file = fullpath(sdir, 'rawhtml.txt')
            if not(path.exists(file) and os.stat(file).st_size):
                param = {}
                param['alp'] = pl[i-1]
                param['dir'] = sdir
                args.append((obj, param))
        lenr = len(args)
        if len(args) > 0:
            if lenr >= leni:
                print "The following parts cann't be downloaded:"
                for arg in args:
                    print arg[1]['alp']
                times = -1
                break
            else:
                pool.map(f_start, args)#f_start(args[0])#for debug
        else:
            break
        leni = lenr
    return times


class dic_downloader(downloader):
#MWU downloader
    def __init__(self, diff, name):
        downloader.__init__(self, name)
        self.__session = None
        self.__diff = diff
        if diff == 't':
            self.__ref = 'thesaurus'
        elif diff == 'd':
            self.__ref = 'collegiate'
        elif diff == 'm':
            self.__ref = 'medical'
        else:
            self.__ref = 'unabridged'
        self.__origin = 'http://unabridged.merriam-webster.com'
        self.__base_url = ''.join([self.__origin, '/', self.__ref, '/%2A'])

    @property
    def diff(self):
        return self.__diff

    @property
    def base_url(self):
        return self.__base_url

    @property
    def ref(self):
        return self.__ref

    @property
    def parts(self):
        if self.__diff == 't':
            pl = [(i*2100, (i+1)*2100) for i in xrange(0, 11)]
            pl.append((23100, 25855))
        elif self.__diff == 'd':
            pl = [(i*2100, (i+1)*2100) for i in xrange(0, 48)]
            pl.append((100800, 102560))
        elif self.__diff == 'm':
            pl = [(i*2100, (i+1)*2100) for i in xrange(0, 19)]
            pl.append((39900, 41812))
        else:
            pl = [(i*2100, (i+1)*2100) for i in xrange(0, 142)]
            pl.append((298200, 299701))
        return pl

    @property
    def session(self):
        return self.__session

    def login(self):
        HEADER = 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.102 Safari/537.36'
        url = 'http://dictionary.eb.com/login'
        param = {'target': '%2F', 'username': '***', 'password': '***'}
        self.__session = requests.Session()
        self.__session.headers['User-Agent'] = HEADER
        self.__session.headers['Origin'] = 'http://dictionary.eb.com'
        self.__session.headers['Referer'] = 'http://dictionary.eb.com/failedlogin'
        r = self.__session.post(url, data=param)
        if r.status_code != 404:
            print self.__session.cookies
            self.__session.headers['Origin'] = self.__origin
            self.__session.headers['Referer'] = self.__base_url
        else:
            print r.text
            self.__session = None

    def logout(self):
        pass

    def getpage(self, link, base_url):
        return getpage(self.__session, link, base_url)

    def postdata(self, start, show):
        param = {'start': str(start), 'show': str(show), 'ref': self.__ref, 'expanded': 'yes'}
        r = self.__session.post(self.__base_url, data=param, timeout=100)
        del self.__session.cookies['recent_lookups']
        if r.status_code == 200:
            return r.content
        else:
            return None

    def __preformat(self, page):
        p = re.compile(r'[\n\r]+')
        page = p.sub(r'', page)
        n = 1
        while n:
            p = re.compile(r'\t+|&nbsp;|\s{2,}')
            page, n = p.subn(r' ', page)
        p = re.compile(r'(</?)strong(?=[^>]*>)')
        page = p.sub(r'\1b', page)
        return page

    def __repimg(self, m, base_url):
        rpath, fnm = m.group(2), m.group(3)
        file =''.join([self.DIC_T, path.sep, self.__diff, 'p', path.sep, fnm])
        if not path.exists(fullpath(file)):
            dump(self.getpage(''.join([rpath, fnm]), base_url), file, 'wb')
        if rpath.find('/med') > -1:
            cls = 'cuj'
        elif rpath.find('/math') > -1:
            cls = 's6p'
        else:
            cls = 'tvb'
        return ''.join([m.group(1), '"p/', fnm, '" class="', cls, '"'])

    def makeword(self, page, words):
        page = self.__preformat(page)
        p = re.compile(r'<!--\s*HEADWORD\s*-->\s*(<div class="wrapper">\s*<div\s+class="hdword">.+?</div>)\s*<!--', re.I)
        m1 = p.search(page)
        if self.__diff == 't':
            p = re.compile(r'(<div id="mwEntryData">.+?)\s*<!--\s*/SECTION\s*-->', re.I)
        elif self.__diff in ['d', 'u', 'm']:
            p = re.compile(r'(?:<div[^<>]*>[\n\r\s]*){7}<div class="corner-bottom-right">[\n\r\s]*(.+?)\s*<div style="margin-top:0;?">\s*<a href="/info/pronsymbols\.html">\s*Pronunciation\s+Symbols', re.I)
        m2 = p.search(page)
        if not m2 or m2.group(1).find('id="mwEntryData"')<0:
            return None
        p = re.compile(r'<div class="hdword">\s*(.+?)\s*</div>', re.I)
        word = p.search(m1.group(1)).group(1).replace('&amp;', '&').replace('&#183;', '').replace('\xE2\x80\x93', '-')
        worddef = ''.join([m1.group(1), m2.group(1)]).strip()
        if self.__diff != 't':
            p = re.compile(r'(?<=<img )[^<>]*?(src=)"(/art/(?:dict|mwu|med)/)thumb/([^<>/"]+)[^<>]+(?=>)', re.I)
            worddef = p.sub(lambda m: self.__repimg(m, self.__origin), worddef)
            p = re.compile(r'(?<=<img )[^<>]*?(src=)"(/math/)([^<>/"]+)[^<>]+(?=>)', re.I)
            worddef = p.sub(lambda m: self.__repimg(m, 'http://www.merriam-webster.com'), worddef)
        words.append((word, worddef))
        return self.strip_key(word)

    def strip_key(self, word):
        p = re.compile(r'<sup[^<>]*>\s*\d+\s*</sup>')
        word = p.sub(r'', word)
        p = re.compile(r'<sub[^<>]*>\s*(\d+)\s*</sub>')
        word = p.sub(r'\1', word)
        p = re.compile(r'<span class="(?:unicode|fr|sc)">([^<>]*)</span>', re.I)
        return p.sub(r'\1', word).strip()

    def __replink(self, m, crefs):
        ref = urllib.unquote(m.group(2).replace('+', ' ')).strip().lower()
        word = m.group(4)
        if ref.startswith('http://') or ref.startswith('/'):
            return m.group(0)
        elif word.lower() in crefs:
            ref = word
        elif ref in crefs:
            ref = crefs[ref]
        else:
            return ''.join(['<span>', word, '</span>'])
        return ''.join([m.group(1), 'entry://', ref.replace('/', '%2F'), '"', m.group(3).rstrip(), '>', word, m.group(5)])

    def __repsc(self, m):
        sc = m.group(1)
        if sc.find('="n6n"')<0:
            return ''.join(['kud', sc])
        else:
            return ''.join(['muj', sc])

    def __rm_div(self, line):
        p = re.compile(r'<(?=div\b)', re.I)
        n = len(p.findall(line))
        p = re.compile(r'<(?=/div>)', re.I)
        line = p.sub(r'<_', line, n).replace('</div>', '').replace('<_/div>', '</div>')
        return line

    def __reptb(self, m):
        title = m.group(1).lower()
        if title == 'synonyms':
            cls = 'tec'
        elif title == 'related words':
            cls = 'a3d'
        elif title == 'near antonyms':
            cls = 'ycv'
        elif title == 'antonyms':
            cls = 'poy'
        elif title == 'phrases':
            cls = 'foa'
        else:
            print title
        return ''.join(['<div class="', cls, '"><span>', title, '</span></div>'])

    def __reppr(self, m):
        pr = m.group(1)
        p = re.compile(r'(<span class="pr">\s*)\\([^\\]+)\\(?=\s*</span>)', re.I)
        return p.sub(r'\1/\2/', pr)

    def __regvr(self, vars, k, links):
        p = re.compile(r'(?:^>|<b[^>]*>)([^:]+?)</b>')
        q = re.compile(r'</?[^<>]+>|\s*&#183;\s*|\xC2\xB7')
        for vr in vars:
            for b in p.findall(vr):
                b = q.sub(r'', b).strip().replace('\xE2\x80\x93', '-')
                if b.lower() != k.lower():
                    links[b] = k

    def __reprlt(self, m, k, links):
        pos = m.group(3)
        self.__regvr([pos], k, links)
        p = re.compile(r'(</b>|/\s*</span>)\s*<em>([^<>]+)</em>(?=\s*(?:</|,|<span|$))', re.I)
        rlt = ''.join([m.group(1), m.group(2), ' class="ikf"', p.sub(r'\1, <span class="wsm">\2</span>', pos)])
        p = re.compile(r'(?<=<span class="wsm">)([^<>]+)(?=</span>)', re.I)
        q = re.compile('(?<=\s)(or)(?=\s)')
        return p.sub(lambda m: q.sub(r'<i>\1</i>', m.group(1)), rlt)

    def __adjwp(self, m):
        pr = m.group(1)
        p = re.compile(r'(;\s*|,\s+)', re.I)
        pr = p.sub(r'<span class="k54">\1 </span>', pr)
        p = re.compile('\s*<em>', re.I)
        return p.sub('<em> ', pr)

    def __repli(self, m):
        ul = m.group(1)
        p = re.compile(r'(<li[^<>]*>)(.+?)(?=</li>)', re.I)
        return p.sub(r'\1<q>\2</q>', ul)

    def __repqc(self, m):
        ci = m.group(1)
        pos = ci.rfind('\xE2\x80\x94')
        return ''.join([ci[:pos], ' ', '</q><cite>', ci[pos:], '</cite>', ])

    def __repsq(self, m, key):
        text = ''.join(['<q>', m.group(2)])
        p = re.compile(r'(?<=[^\w\d\s,;:])\s*(\xE2\x80\x94.+?)\s*(?=$)', re.I)
        text, n = p.subn(self.__repqc, text)
        if not n:
            p = re.compile(r'(\xE2\x80\x94\s*[A-Z].+?)\s*(?=$)')
            text, n = p.subn(self.__repqc, text)
            if not n:
                p = re.compile(r'(\xE2\x80\x94\s*<em>\s*([^<>]+)\s*</em>)(?=\s*$)', re.I)
                text, n = p.subn(lambda s: ''.join(['</q><cite>', s.group(1), '</cite>']) if s.group(2).lower()!=key.lower() else s.group(1), text)
                if not n:
                    text = ''.join([text, '</q>'])
        return ''.join([m.group(1), text, m.group(4), m.group(3)])

    def __repqt(self, m):
        text = m.group(1)
        p = re.compile(r'(?<=[^\w\d\s,;:])\s*(\xE2\x80\x94.+?)\s*</(?=$)', re.I)
        text = p.sub(self.__repqc, ''.join(['<q class="iqs">', text]))
        return ''.join(['<div>', text, '</div>'])

    def __rmci(self, m):
        p = re.compile(r'</?cite>', re.I)
        return p.sub(r'', m.group(1))

    def __repdt(self, m):
        tt = m.group(1).lower()
        if tt == 'synonyms':
            cls = 'ltp'
        elif tt == 'antonyms':
            cls = 'jcz'
        elif tt == 'related words':
            cls = 'yqg'
        elif tt == 'near antonyms':
            cls = 'lpn'
        elif tt == 'phrases':
            cls = 'zmf'
        else:
            print tt
        return ''.join([' class="', cls, '">', m.group(1)])

    def __make_tbl(self, reft):
        p = re.compile('^\w+', re.I)
        nm = p.search(reft).group(0).lower()
        hd = ''.join(['<div><div style="margin-bottom:1em;color:maroon;font-family:Georgia,Times;font-size:120%"><b>', reft, '</b></div>'])
        if nm == 'language':
            tbl = ''.join(['<div style="margin-left:1ex;font-family:Helvetica">',
            '<table style="border-collapse:collapse"border="1"bordercolor="#CCC">',
            '<tr><th colspan=2>HOME LANGUAGES WITH OVER<BR>FORTY MILLION SPEAKERS<sup>1</sup></tr>',
            '<tr><th>LANGUAGE</th><th>MILLIONS</th></tr>'])
            trs = ''.join([''.join(['<tr><td>', lc, '</td><td align="center">', rc, '</td></tr>'])
            for lc, rc in [('Mandarin Chinese', '865'), ('English', '334'), ('Spanish', '283'), ('Arabic', '197'),
            ('Bengali', '181'), ('Hindi and Urdu', '172'), ('Portuguese', '161'), ('Russian', '156'),
            ('Japanese', '125'), ('German', '104'), ('Wu Chinese', '94'), ('Panjabi', '76'),
            ('Javanese', '76'), ('Telugu', '72'), ('Cantonese', '70'), ('Korean', '70'), ('Marathi', '68'),
            ('Italian', '68'), ('Tamil', '65'), ('French', '65'), ('Vietnamese', '61'), ('Awadhi<sup>2</sup>', '61'),
            ('Bhojpuri', '58'), ('Southern Min Chinese<sup>3</sup>', '55'), ('Turkish', '52'), ('Ukrainian', '50'),
            ('Thai and Lao', '47'), ('Polish', '42'), ('Gujarati', '41'), ('Persian', '40')]])
            cmt = ''.join(['</table><div><sup>1</sup>Compiled by William W. Gage, using information supplied in '
            '<i style="font-family:Georgia">Ethnologue: Languages of the World</i>, 11th ed. (Dallas: Summer Institute'
            ' of Linguistics, 1988). Home language used here means the language usually spoken at home.</div>'
            '<div><sup>2</sup>Indo-Aryan language of eastern and central Uttar Pradesh.</div>'
            '<div><sup>3</sup>Group of Chinese dialects especially of Xiamen, Shantou, and Taiwan.</div></div>'])
            return ''.join([hd, tbl, trs, cmt])
        else:
            if reft == 'ship\'s bells table':
                nm = 'bell'
            return ''.join([hd, '<img src="p/', nm, '.jpg"style="margin-left:1em;max-width:90%"></div>'])

    def __reptl(self, m, tables):
        p = re.compile(r'((?:View the\s*)?)(.+)', re.I)
        n = p.search(m.group(3))
        reft = n.group(2)
        if not reft in tables:
            print reft, "is generated"
            tables[reft] = self.__make_tbl(reft)
        return ''.join(['class="ga0" ', m.group(1), 'entry://', reft, m.group(2), m.group(3)])

    def __make_illu(self, link, illu):
        if not self.session:
            self.login()
        try:
            page = self.getpage(link, self.__origin)
        except Exception, e:
            return None
        if not page:
            return None
        page = self.__preformat(page).strip()
        p = re.compile(r'<div class="well content-body">(.+?)</div>', re.I)
        m = p.search(page)
        if not m:
            return None
        p = re.compile(r'<body[^<>]*>\s*(?:<script[^>]*>.*?</script>\s*)?(.+?)\s*</body>', re.I)
        m = p.search(m.group(1))
        if not m:
            return None
        p = re.compile(r'(</?)it?(?=>)', re.I)
        body = p.sub(r'\1b', m.group(1))
        p = re.compile('(?<=<img src=)[\'"]([^<>\'"]+?\.gif)[^>]+(?=>)', re.I)
        fnm = p.search(body).group(1)
        body = p.sub(r'"p/\1"  style="margin:1em;max-width:90%"', body)
        folder = ''.join([self.DIC_T, path.sep, self.diff, 'p', path.sep])
        imgpath = fullpath(folder)
        if not path.exists(imgpath):
            os.mkdir(imgpath)
        if not path.exists(''.join([imgpath, fnm])):
            dump(self.getpage(''.join([link[:link.rfind('/')+1], fnm]), self.__origin), ''.join([folder, fnm]), 'wb')
        return ''.join(['<div><div style="color:#CA0403;font-family:Georgia,Times;font-size:120%"><b>', illu, '</b></div>', body, '</div>'])

    def __repil(self, m, illusts):
        illu = m.group(4)
        if not illu in illusts:
            print "generating", illu
            entry = self.__make_illu(m.group(2), illu)
            if entry:
                illusts[illu] = entry
            else:
                print "Page %s is empty" % m.group(2)
                return m.group(0)
        return ''.join(['class="gpo" ', m.group(1), 'entry://', illu, m.group(3), m.group(4)])

    def set_repcls(self):
        self.__trs_tbl ={'div': {'wrapper': 'jk6', 'hdword': 'iye', 'fl': 'eyx',
        'sense-block-one': 'b8o', 'sblk': 'j1b', 'r': 'slu', 'sub-well': 'kmv',
        'pron': 'fwb', 'section-content': 'cdw', 'us': 'bfw', 'dr': 'wgh',
        'fl-xtra': 'fg9', 'section-content etymology': 'oje', 'ss': 'qms',
        'vt': 'm6j', 'd dxnl': 'mxc', 'dx': 'nh6', 'sr synonym-discussion': 'kyc',
        'section inf-forms': 'c6t', 'section variants': 'vr0', 'd': 'uay',
        'sub-well usage-discussion': 'rgk'},
        'span': {'ssens': 'xsx', 'vi': 'zz0', 'sc': 'lcw', 'sn': 'lsz',
        'text': 'fzy', 'qword': 'sq2', 'pr': 'byp', 'ph': 'plh', 'fr': 'co8',
        'ibar': 'k7r', 'unicode': 'u6u', 'illust': 'ac3', 'code_uhorn': 'uy4',
        'code_ibartild': 'iba', 'code_ohornac': 'uqh', 'code_hrev': 'oav',
        'code_openocrc': 'ejl', 'vi last-vi': 'zfn', 'ssens snblk': 'z8i',
        'ssens snblk unblk': 'oxy', 'called-also': 'gkc', 'utxt': 'epi',
        'snote': 'zb4', 'mark': 'puq', 'ssens subsense': 'mhv', 'sgram': 'rkt',
        'dx_def': 'ond', 'et_snote': 'obg', 'ssens snblk subsense': 'ipn',
        'psl-container': 'fiz', 'phrase': 'wym', 'gloss': 'rm1', 'dx': 'qf5',
        'in': 'rjq', 'ssens snblk unblk subsense': 'zrn', 'et': 'o8m',
        'dxn': 'znu', 'psl': 'azf', 'ix': 'bx8', 'in-more': 'wnr', 'set': 'lfe',
        'breve': 'rzr', 'ua': 'xpv', 'p': 'smp'},
        'b': {'va': 'hfo'}, 'h2': {'toggle': 't2h'}, 'ul': {'examples': 'jwt'},
        'ol': {'item-list': 'bsf'}, 'em': {'it': 'hcs', 'vl': 'ima'},
        'a': {'formulaic': 'eb0', 'more-at': 'k9i', 'd_link': 'qx0',
        'cat': 'efp', 'et_link': 'grt', 'sr': 'wgn', 'dx': 'pxk', 'ct': 'kyl',
        'dxnl': 'fng', 'ur': 'bol', 'i_link': 'zxw', 'lookup': 'zke'}}

    def __repcls(self, m):
        tag = m.group(1)
        cls = m.group(3)
        if tag in self.__trs_tbl and cls in self.__trs_tbl[tag]:
            return ''.join([tag, m.group(2), self.__trs_tbl[tag][cls]])
        else:
            return m.group(0)

    def format_entry(self, key, line, crefs, links, appendix):
        line = line.replace('\xC2\xA0', ' ')
        p = re.compile(r'xmlns:mwref="http://www\.m-w\.com/mwref"\s*', re.I)
        line = p.sub(r'', line)
        p = re.compile(r'(<\w+)\s+(?=>)', re.I)
        line = p.sub(r'\1', line)
        p = re.compile(r'(<span class="vi(?:\s+last-vi)?">)\s*&lt;\s*(.+?)\s*&gt;\s*(</span>\s*)(\.?)', re.I)
        line = p.sub(lambda m: self.__repsq(m, key), line)
        p = re.compile(r'(<a[^>]*>)\s*(<sup>\d+</sup>)(.+?)\s*(</a>)', re.I)
        line = p.sub(r'\1\3\4\2', line)
        p = re.compile(r'(<sup>\d+</sup>)(\s*<a[^>]*>.+?</a>)', re.I)
        line = p.sub(r'\2\1', line)
        p = re.compile(r'(<(span|em)[^>]*>)\s*(<sup>\d+</sup>)([^<>]+?)\s*(?=</\2>)', re.I)
        line = p.sub(r'\1\4\3', line)
        p = re.compile(r'(<a [^<>]*?href="[^<>"]+?)\[\d+\](?="[^<>]*>)', re.I)
        line = p.sub(r'\1', line)
        p = re.compile(r'<a[^>]*>(\s*[\[\]\(\)]\s*)</a>', re.I)
        line = p.sub(r'\1', line)
        p = re.compile(r'([\[\(]\s*)(</a>)', re.I)
        line = p.sub(r'\2\1', line)
        p = re.compile(r'(<a (?:class="[^<>"]+"\s*)?href=")(?:/collegiate/|/unabridged/|id%3A)?([^<>"]+?)\s*"((?:\s*class="[^<>"]+")?)\s*>\s*(.+?)\s*(</a>)', re.I)
        line = p.sub(lambda m: self.__replink(m, crefs), line)
        p = re.compile(r'<a href="/[^<>]+>\s*(<img [^<>]+>)\s*</a>', re.I)
        line = p.sub(r'\1', line)
        p = re.compile(r'<a href="(?:/collegiate/)"/>,?\s*(?=<a href=")', re.I)
        line = p.sub(r'', line)
        p = re.compile(r'<div( class=")snum(">\d+</)div>\s*(<div[^>]*>)\s*', re.I)
        line = p.sub(r'\3<span\1n6n\2span>', line)
        p = re.compile(r'(?<=<div class=")scnt(">.+?</div>)', re.I)
        line = p.sub(self.__repsc, line)
        p = re.compile(r'(?<=<div class="hdword">)\s*(<sup>\d+</sup>)(.+?)\s*(?=</div>)', re.I)
        line = p.sub(r'\2\1', line)
        if self.__diff == 't':
            line = self.__rm_div(line)
            p = re.compile(r'(?<=<div>)\s*<b>\s*([^<>]+?)\s*</b>\s*', re.I)
            line = p.sub(self.__reptb, line)
            p = re.compile(r'(?<=<span class="ssens">)(.+?)\s*(?=<span class="vi(?:\s+last-vi)?">)', re.I)
            line = p.sub(r'<span class="ltr">\1 <img src="x.png"onclick="vrh.c(this)"></span>', line)
            p = re.compile(r'\[\s*(</(a|span)>)\s*<em[^>]*>([^<>]+)</em>\s*(<\2[^<>]*>)\s*\]', re.I)
            line = p.sub(r'\1<span class="bld"><span>(</span>\3<span>)</span></span>\4', line)
            p = re.compile(r'(\(\s*<em)(?=>)', re.I)
            line = p.sub(r'\1 class="hcs"', line)
        else:
            jsn = 'dcq' if self.__diff=='m' else 'dzp'
            if self.__diff == 'u':
                p = re.compile(r'<div class="scnt"/>', re.I)
                line = p.sub(r'', line)
            p = re.compile(r'(<div class="[^<>"]+")\s*style="[^<>]*"\s*(?=>)', re.I)
            line = p.sub(r'\1', line)
            p = re.compile(r'<div class="section variants">(.+?)</div>', re.I)
            self.__regvr(p.findall(line), key, links)
            p = re.compile(r'\s*<div class="section" data-id="definition">\s*(?:<div id="wordclick" class="wordclick">\s*)?(?=<div [^>]*?id="mwEntryData")', re.I)
            line = p.sub(r'', line)
            if self.__diff == 'm':
                p = re.compile(r'(<div class="d">)\s*<div class="d">\s*', re.I)
                line = p.sub(r'\1', line)
                p = re.compile(r'(?<=<div)>\s*<em>\s*([^<>]+)\s*</em>\s*(?=</div>)', re.I)
                line = p.sub(r' class="m6j">\1', line)
            p = re.compile(r'(<div\s+[^<>]*?id="mwEntryData"[^<>]*>.+?)\s*(?:<!--\s*SECTION\s*-->|$)', re.I)
            line = p.sub(lambda m: self.__rm_div(m.group(1)), line)
            p = re.compile(r'<(script|style)\b.+?</\1>', re.I)
            line = p.sub(r'', line)
            p = re.compile(r'<div class="img-label">\s*<img src="[^<>"]+?update-(?:new|full)\.jpg"[^<>]*>\s*</div>', re.I)
            line = p.sub(r'', line)
            p = re.compile(r'<a href="#" onclick="return au\(\'\s*(\w[^\']*)\'[^<>]+>.+?</a>', re.I)
            line = p.sub(''.join(['<img src="sp.png" class="sxa" onclick="', jsn, '.v(this,\'', r'\1', '\')">']), line)
            p = re.compile(r'<a href="#" onclick="return au\(\'.+?</a>', re.I)
            m = p.search(line)
            if m:
                print "Remove wrong audio link: %s, %s" % (key, m.group(0))
                line = p.sub(r'', line)
            line = p.sub(r'', line)
            p = re.compile(r'(?<=<div class="pron">)\s*(<span class="pr">\s*\\.+?\\\s*</span>)\s*(?=</div>)', re.I)
            line = p.sub(self.__reppr, line)
            p = re.compile(r'(<div class="pron">.+?)\s*(/\s*</span>\s*</div>)\s*<div class="audio">\s*(.*?)\s*</div>', re.I)
            line = p.sub(r'\1 \3\2', line)
            p = re.compile(r'<div class="audio">\s*</div>', re.I)
            line = p.sub(r'', line)
            p = re.compile(r'\s*(</div>)<div class="audio">\s*(<img src="sp.png"[^<>]+>)\s*</div>', re.I)
            line = p.sub(r' \2\1', line)
            p = re.compile(r'(?<=<span class="pr">)\s*\\([^\\]+)\\\s*(</span>\s*)((?:\s*<img src="sp.png"[^<>]+>\s*?)+)', re.I)
            line = p.sub(r'/\1 \3/\2', line)
            p = re.compile(r'(\s*)((?:\s*<img src="sp.png"[^<>]+>\s*?)+)(\s*<span class="pr">)\s*\\([^\\]+)\\(?=\s*</span>)', re.I)
            line = p.sub(r'\1\3/\4 \2/', line)
            p = re.compile(r'(<span class="pr">\s*)\\([^\\]+)\\(?=\s*</span>)', re.I)
            line = p.sub(r'\1/\2/', line)
            p = re.compile(r'\s+/\s*(?=</span>)', re.I)
            line = p.sub(r'/', line)
            p = re.compile(r'(?<=<span class="pr">)(\s*/.+?/\s*)(?=</span>)', re.I)
            line = p.sub(self.__adjwp, line)
            p = re.compile(r'(<div class="d?r">)\s*\xE2\x80\x94\s*(<b)(.+?</div>)', re.I)
            line = p.sub(lambda m: self.__reprlt(m, key, links), line)
            p = re.compile(r'(?<=<div class="f1">)([^<>]+)(?=</div>)', re.I)
            q = re.compile('(?<=\s)(or)(?=\s)')
            line = p.sub(lambda m: q.sub(r'<i>\1</i>', m.group(1)), line)
            p = re.compile(r'\s*&#183;\s*|\xC2\xB7', re.I)
            line = p.sub(r'<span></span>', line)
            p = re.compile(r'<(span|div) class="break">\s*</\1>', re.I)
            line = p.sub(r'', line)
            p = re.compile(r'<span class="ssens">\s*(<em class="sn">\s*(?:[^<>]|a[^<>])\s*</em>\s*)</span>\s*(<span class="ssens">)\s*', re.I)
            line = p.sub(r'\2\1', line)
            p = re.compile(r'\s*(<span class="ssens(?:\s+snblk)?">)\s*<b>:</b>\s*', re.I)
            line = p.sub(r' \1', line)
            p = re.compile(r'(<span class="ssens(?:\s+snblk)?">)\s*', re.I)
            line = p.sub(r' \1', line)
            p = re.compile(r'<em( class="sn">\s*(?:[^<>]|a[^<>])\s*</)em>', re.I)
            line = p.sub(r'<span\1span>', line)
            p = re.compile(r'(<a href=")(?:/collegiate/|/unabridged/|id%3A)?([^<>"]+?)(?:\[\d+\])?"(\s*class="d_link")\s*onclick="_gaq\.push[^<>]+>(.+?)(</a>)', re.I)
            line = p.sub(lambda m: self.__replink(m, crefs), line)
            p = re.compile(r'<li([^<>]*>)\s*&lt;\s*(.+?)\s*&gt;\s*(</)li>', re.I)
            line = p.sub(r'<div\1<q>\2</q>\3div>', line)
            p = re.compile(r'<li>\s*(?:<span>\s*)?<span class="quote">(.+?</)span>\s*(?:</span>\s*)?</li>', re.I)
            line = p.sub(self.__repqt, line)
            p = re.compile(r'(<ul class="examples">.+?</ul>)', re.I)
            line = p.sub(self.__repli, line)
            p = re.compile(r'(?<=<div class=")[^<>]*?data-id="examples"[^<>]*(>\s*<h2 class="toggle">\s*<span class="text">Examples).+?\s*(</span>)\s*(?=</h2>)', re.I)
            line = p.sub(''.join([r'xkm"\1\2<span class="izv" onclick="', jsn, '.x(this)"></span>']), line)
            p = re.compile(r'(?<=<div class=")[^<>]*?data-id="origin"[^<>]*(>\s*<h2 class="toggle">\s*<span class="text">Origin).+?\s*(</span>)\s*(?=</h2>)', re.I)
            line = p.sub(''.join([r'oxl"\1\2<span class="izv" onclick="', jsn, '.x(this)"></span>']), line)
            p = re.compile(r'(?<=<div class=")[^<>]*?data-id="related-to"[^<>]*(>\s*<h2 class="toggle">\s*<span class="text">Related).+?\s*(</span>)\s*(?=</h2>)', re.I)
            line = p.sub(''.join([r'ihb"\1 words\2<span class="izv" onclick="', jsn, '.x(this)"></span>']), line)
            p = re.compile(r'(?<=<div class=")[^<>]*?data-id="rhymes"[^<>]*(>\s*<h2 class="toggle">\s*<span class="text">Rhymes).+?\s*(</span>)\s*(?=</h2>)', re.I)
            line = p.sub(''.join([r'rmr"\1\2<span class="izv" onclick="', jsn, '.x(this)"></span>']), line)
            p = re.compile(r'(?<=<div class=")[^<>]*?data-id="first-known-use"[^<>]*(>\s*<h2 class="toggle">\s*<span class="text">First) Known Use.+?\s*(</span>)\s*(?=</h2>)', re.I)
            line = p.sub(''.join([r'fkf"\1 known use\2<span class="izv" onclick="', jsn, '.x(this)"></span>']), line)
            p = re.compile(r'(?<=<div class=")[^<>]*?data-id="usage"[^<>]*(>\s*<h2 class="toggle">\s*<span class="text">Usage) Discussion.+?\s*(</span>)\s*(?=</h2>)', re.I)
            line = p.sub(''.join([r'uea"\1 discussion\2<span class="izv" onclick="', jsn, '.x(this)"></span>']), line)
            p = re.compile(r'(<div class="[^<>]*?data-id="artwork"[^<>]*>)\s*<h2 class="toggle">\s*<span class="text">Illustration.+?\s*</span>\s*</h2>', re.I)
            line = p.sub(r'\1', line)
            p = re.compile(r'<!--[^<>]+?-->')
            line = p.sub('', line)
            p = re.compile(r'<div class="[^<>]*?data-id="artwork"[^<>]*>\s*<div class="section-content">\s*<div class="sub-well">\s*(<img[^<>]+>)(?:\s*</div>\s*){3}', re.I)
            m = p.search(line)
            q = re.compile(r'(?=<div class="d">)', re.I)
            if m:
                line = q.sub(m.group(1), line, 1)
            line = p.sub(r'', line)
            p = re.compile(r'<div class="[^<>]*?data-id="artwork"', re.I)
            if p.search(line):
                raise AssertionError('%s: img has not formatted' % key)
            p = re.compile(r'<div class="accordion-heading">\s*<a[^<>]+>\s*more\s*</a>\s*</div>', re.I)
            line = p.sub(r'', line)
            p = re.compile(r'(<div) id="accordion-(examples|related-to)" class="accordion"(>\s*<div) class="accordion-group"(>\s*<div )id="\2-more" class="accordion-body collapse"(?=>)', re.I)
            line = p.sub(r'\1\3\4class="l1s"', line)
            p = re.compile(r'(?<=<dl><dt)>([^<>]+?)(?=:?\s*</dt>)', re.I)
            line = p.sub(self.__repdt, line)
            p = re.compile(r'<em( class=")ssn(">\(\d+\)</)em>', re.I)
            line = p.sub(r'<span\1okj\2span>', line)
            p = re.compile(r'(?<=<div class=")syn synonym-discussion(">\s*(?:<div>)?\s*)(?:<b>Synonym Discussion:</b>)?\s*(.+?)(?=</div>)', re.I)
            line = p.sub(r'ocp\1<span class="iom">Synonym discussion</span><div class="xtc">\2</div>', line)
            p = re.compile(r'(?<=<a )(?:class="table-link"\s*)?(href=")/table/(?:dict|unabridged)/[^<>]+\.htm(">)\s*([^<>]+?)\s*(?=</a>)', re.I)
            line = p.sub(lambda m: self.__reptl(m, appendix[0]), line)
            if self.__diff == 'm':
                p = re.compile(r'(?<=<a )(href=")/table/dict/[^<>]+\.htm" class="lookup table(">)\s*([^<>]+?)\s*(?=</a>)', re.I)
                line = p.sub(lambda m: self.__reptl(m, appendix[0]), line)
                line = line.replace('<img class="math formula" src="/math/"/>', '')
            p = re.compile(r'(?<=<a )(href=")(/art/(?:dict|unabridged)/[^<>]+\.htm)(">)\s*([^<>]+?)\s*(?=</a>)', re.I)
            line = p.sub(lambda m: self.__repil(m, appendix[1]), line)
            p = re.compile(r'<a href="/art/(?:dict|unabridged)/[^<>]+\.htm">([^<>]+?)</a>', re.I)
            line = p.sub(r'<span>\1</span>', line)
            p = re.compile(r'<em>(of [^<>]+)</em>(?=\s*<b>\s*:)', re.I)
            line = p.sub(r'<span class="beq">\1</span>', line)
            p = re.compile(r'(?<=</div>)\s*<em>(of [^<>]+)</em>(?=\s*<div)', re.I)
            line = p.sub(r'<span class="beq">\1</span>', line)
            p = re.compile(r'(">\s*)<em>(of [^<>]+)</em>(?=\s*(?:</span>|<div|<em|<span class="sn">))', re.I)
            line = p.sub(r'\1<span class="beq">\2</span>', line)
            p = re.compile(r'<em>([^<>]+)</em>(?=\s*<b>\s*:)', re.I)
            line = p.sub(r'<span class="lej">\1</span>', line)
            p = re.compile(r'(?<=</div>)\s*<em>([^<>]+)</em>(?=\s*<div)', re.I)
            line = p.sub(r'<span class="lej">\1</span>', line)
            p = re.compile(r'(">\s*)<em>([^<>]+)</em>(?=\s*(?:</span>|<div|<em|<span class="sn">))', re.I)
            line = p.sub(r'\1<span class="lej">\2</span>', line)
            p = re.compile(r'(<span class="lej">[^<>]+?,\s*</span>\s*)<em>(of [^<>]+)</em>', re.I)
            line = p.sub(r'\1<span class="beq">\2</span>', line)
            p = re.compile(r'(<span class="lej">[^<>]+?,\s*</span>\s*)<em>([^<>]+)</em>', re.I)
            line = p.sub(r'\1<span class="lej">\2</span>', line)
            if self.__diff == 'u':
                p = re.compile(r'<span class="ix"/>', re.I)
                line = p.sub(r'', line)
                p = re.compile(r'(?<=<div class=")section" data-id="ur"[^>]*(?=>)', re.I)
                line = p.sub(r'nvq"', line)
                p = re.compile(r'(?<=<div class=")section" data-id="table"[^>]*(?=>)', re.I)
                line = p.sub(r'y4l"', line)
                line = line.replace('<a href="/unabridged/"/>', '')
        p = re.compile(r'(</(?:q|cite)>)\s*(</span>)\s*([,;\.])', re.I)
        line = p.sub(r'\1\3\2', line)
        p = re.compile(r'\[\s*<em[^>]*>([^<>]+)</em>\s*\]', re.I)
        line = p.sub(r'<span class="bld"><span>(</span>\1<span>)</span></span>', line)
        p = re.compile(r'\(\s*<em[^>]*>(slang)</em>\s*\)', re.I)
        line = p.sub(r'<span class="bld"><span>(</span>\1<span>)</span></span>', line)
        p = re.compile(r'<em( class="sc">.+?</)em>', re.I)
        line = p.sub(r'<span\1span>', line)
        p = re.compile(r'<a [^<>]*?href="(?:/|http://)|<img [^<>]*?src="/', re.I)
        if p.search(line):
            dump(line, 't.txt')#for check&debug
            raise AssertionError('%s : link/img not transformed' % key)
        line = self.cleansp(line)
        p = re.compile(r'(?<=<)(span|div|a|b|h2|ul|em|ol)([^<>]*? class=")([^<>"]+?)\s*(?=")', re.I)
        line = p.sub(self.__repcls, line)
        n = 1
        while n:
            p = re.compile(r'(?<=<)(ol|ul|li)([^<>]*>.+?</)\1(?=>)', re.I)
            line, n = p.subn(r'div\2div', line)
        n = 1
        while n:
            p = re.compile(r'<div>(<div><div[^<>]*>.+?</div></div>)</div>', re.I)
            line, n = p.subn(r'\1', line)
        p = re.compile(r'<div\s+[^<>]*?id="mwEntryData"[^<>]*>', re.I)
        line = p.sub(r'<div class="ewr">', line)
        p = re.compile(r'(<div class="m6j">[^<>]+</div>)\s*(<span class="[^>"]+">)\s*', re.I)
        line = p.sub(r'\1 \2', line)
        line = line.replace('<div class="eyx">', ' <div class="eyx">')
        return line

    def make_entry(self, cn, v):
        if len(v) > 1:
            line = ''
            for sk, sv in sorted(v, key=lambda d: d[0]):
                line = ''.join([line, '<div class="blr">', sv, '</div>'])
        else:
            line = v[0][1]
        ps, pd = r'<img\s+src="', r'\.gif"\s*class="tvb">'
        p = re.compile(''.join([ps, r'([^<>"]+?)', pd]))
        m = p.search(line)
        if m:
            p = re.compile(''.join([ps, m.group(1), pd]))
            line = ''.join([line[:m.end()], p.sub(r'', line[m.end():])])
        nojs = (line.find('onclick="')<0)
        if self.diff == 't':
            id, cls = '', 'm3s'
            src = '' if nojs else ''.join(['<img src="c.png"style="display:none">',
            '<script type="text/javascript">var vrh=(function(){return{c:function(c){var p=c.parentNode.parentNode;var d=p.getElementsByTagName("div");var r=/\w+(?=\.png$)/;for(var i=0;i<d.length;i++)',
            'if(d[i].style.display=="block"){c.src=c.src.replace(r,"x");d[i].style.display="none";}else{c.src=c.src.replace(r,"c");d[i].style.display="block";}}}}());</script>'])
        else:
            jsn = 'dzp'
            if self.diff == 'u':
                id = 'mjy'
            elif self.diff =='d':
                id = 'bty'
            else:
                id, jsn = 'mxs', 'dcq'
            cls, src = 'mm2', '' if nojs else ''.join(['<script type="text/javascript"src="w1.js"></script><script>if(typeof(', jsn, ')=="undefined"){var _l=document.getElementsByTagName("link");var _r=/',
            self.DIC_T, '.css$/;for(var i=_l.length-1;i>=0;i--)with(_l[i].href){var _m=match(_r);if(_m&&_l[i].id=="', id, '"){document.write(\'<script src="\'+replace(_r,"w1.js")+\'"type="text/javascript"><\/script>\');break;}}}</script>'])
        id = '' if (self.diff=='t' or nojs) else ''.join(['id="', id, '"'])
        line = ''.join(['<link ', id, 'rel="stylesheet"href="', cn, '.css"type="text/css"><div class="', cls, '">', line, src, '</div>'])
        return line

    def make_pron(self, appendix):
        title = 'Pronunciation Symbols'
        if len(appendix)>2 and appendix[2].keys()[-1] == title:
            page = appendix[2].values()[-1]
        else:
            page = self.getpage('/info/pronsymbols.html', self.__origin)
            page = self.__preformat(page).replace('\\', '/')
            p = re.compile(r'(<div class="pron-column">.+?</div>)', re.I)
            page = ''.join(p.findall(page)).replace('"pron-column"', '"kzo"').replace('"unicode"', '"xt5"')
            folder = ''.join([self.DIC_T, path.sep, 'v', path.sep])
            imgpath = fullpath(folder)
            if not path.exists(imgpath):
                os.mkdir(imgpath)
            p = re.compile(r'(?<=<img src=")(images/)([^>]+\.gif)', re.I)
            for rp, fnm in p.findall(page):
                if not path.exists(''.join([imgpath, fnm])):
                    dump(self.getpage(''.join(['/info/', rp, fnm]), self.__origin), ''.join([folder, fnm]), 'wb')
            page = p.sub(r'v/\2', page)
            p = re.compile('</span>\s*(?=\(pronunciation of)', re.I)
            page = p.sub(r'', page)
            p = re.compile(r'(<p>\+.+?</p>)\s*(</div>)', re.I)
            page = self.cleansp(p.sub(r'\2\1', page))
            p = re.compile(r'<a href="#"[^<>]*?onClick="return au\(\'(\w)([^\']+)\'[^<>]+>(.+?</)a>', re.I)
            page = p.sub(''.join(['<span class="wed" onclick="wa0(this,\'', r'\1/\1\2', '\')">', r'\3span>']), page)
            src = ''.join(['<script type="text/javascript">function wa0(c,f){c.setAttribute("onclick","javascript:void(0);");c.style.cursor="default";',
            'var u="http://media.merriam-webster.com/soundc11/"+f+".wav";var b=function(){c.style.cursor="pointer";',
            'c.setAttribute("onclick","wa0(this,\'"+f+"\')");};var t=setTimeout(b,2000);try{with(document.createElement("audio"))',
            '{setAttribute("src",u);onloadstart=function(){clearTimeout(t);};onended=b;play();}}catch(e){}}</script>'])
            page = ''.join(['<link rel="stylesheet"href="PR.css"type="text/css"><div class="rlc"><div class="lcp"><b>Pronunciation Symbols</b></div><p>Click on any button below to hear the pronunciation.</p>', page, src, '</div>'])
            appendix.append({title: page})
        return '\n'.join([title, page, '</>\n'])

    def load_appendix(self, dir):
        appendix = [OrderedDict(), OrderedDict()]
        ap_file = fullpath(''.join([dir, 'appendix.txt']))
        if path.exists(ap_file):
            lns = []
            for ln in fileinput.input(ap_file):
                ln = ln.strip()
                if ln == '</>':
                    if lns[0] == 'Pronunciation Symbols':
                        appendix.append({lns[0]: lns[1]})
                    elif lns[0].endswith('table'):
                        appendix[0][lns[0]] = lns[1]
                    else:
                        appendix[1][lns[0]] = lns[1]
                    del lns[:]
                elif ln:
                    lns.append(ln)
        return appendix

    def dump_appendix(self, dir, fm, appendix):
        if appendix[0]:
            print "%s " % info(len(appendix[0]), 'table'),
            dump(''.join(['\n'.join([k, v, '</>\n']) for k, v in appendix[0].iteritems()]), fm, 'a')
        if appendix[1]:
            print "%s " % info(len(appendix[1]), 'illustration'),
            dump(''.join(['\n'.join([k, v, '</>\n']) for k, v in appendix[1].iteritems()]), fm, 'a')
        fm = ''.join([dir, 'appendix.txt'])
        [appendix[0].update(appendix[i]) for i in xrange(1, len(appendix))]
        dump(''.join(['\n'.join([k, v, '</>\n']) for k, v in appendix[0].iteritems()]), fm)


def is_complete(path, ext='.part'):
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(ext):
                return False
    return True


def merge_d_t(name, dc, th, key):
    th = th.replace('<link rel="stylesheet"href="MWT.css"type="text/css">', '')
    p = re.compile(r'(<link\s[^<>]+>)', re.I)
    dc = p.sub(r'\1<div class="wdu"><div class="nau"><span class="kfh"onclick="javascript:void(0);">Dictionary</span><span class="dt7"onclick="dzp.h(this)">Thesaurus</span></div>', dc, 1)
    p = re.compile(r'(?=<script)', re.I)
    if p.search(dc):
        dc = p.sub(''.join(['</div>', th]), dc, 1)
    else:
        src = ''.join(['<script type="text/javascript"src="w1.js"></script><script>if(typeof(dzp)=="undefined"){var _l=document.getElementsByTagName("link");var _r=/',
        name, '.css$/;for(var i=_l.length-1;i>=0;i--)with(_l[i].href){var _m=match(_r);if(_m&&_l[i].id=="bty"){document.write(\'<script src="\'+replace(_r,"w1.js")+\'"type="text/javascript"><\/script>\');break;}}}</script>'])
        dc = ''.join([dc, th, src, '</div>'])
    return dc


def merge(basedir):
    dir_t = ''.join([basedir, path.sep, 'thesaurus', path.sep])
    file = fullpath('MWT.txt', base_dir=dir_t)
    if not path.exists(file):
        print "Cannot find thesaurus file 'MWT.txt'."
        return
    words, lns, thes = [], [], OrderedDict()
    for ln in fileinput.input(file):
        ln = ln.strip()
        if ln == '</>':
            thes[lns[0].lower()] = (lns[0], lns[1])
            del lns[:]
        elif ln:
            lns.append(ln)
    fnm = ''.join([basedir, path.extsep, 'txt'])
    dir_t = ''.join([basedir, path.sep, 'collegiate', path.sep])
    file = fullpath(fnm, base_dir=dir_t)
    if not path.exists(file):
        print "Cannot find collegiate dictionary file '%s'." % fnm
        return
    fw = open(fullpath(''.join([basedir, path.sep, fnm])), 'w')
    try:
        for ln in fileinput.input(file):
            ln = ln.strip()
            if ln == '</>':
                key, wdef, uk = lns[0], lns[1], lns[0].lower()
                if uk in thes and thes[uk]:
                    if not wdef.startswith('@@@') and not thes[uk][1].startswith('@@@'):
                        wdef = merge_d_t(basedir, wdef, thes[uk][1], lns[0])
                        thes[uk] = None
                fw.write('\n'.join([key, wdef, '</>\n']))
                words.append(key)
                del lns[:]
            elif ln:
                lns.append(ln)
        for k, v in thes.iteritems():
            if v:
                fw.write('\n'.join([v[0], v[1], '</>\n']))
                words.append(v[0])
    finally:
        fw.close()
    print "%s totally." % info(len(words))
    fw = open(fullpath(''.join([basedir, path.sep, 'words.txt'])), 'w')
    fw.write('\n'.join(words))
    fw.close()


if __name__=="__main__":
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("diff", nargs="?", help="choose one from [u/d/t/m][c][f]")
    parser.add_argument("diff2", nargs="?", help="[u][d][t][m] To specify dictionary to format")
    print "Start at %s" % datetime.now()
    args = parser.parse_args()
    if args.diff in ['d', 't', 'u', 'm']:
        diff = args.diff
    elif args.diff == 'f' and args.diff2:
        diff = args.diff2
    elif args.diff != 'c':
        diff = None
        print "Parameter error, choose one from [u/d/t][m][f u/d/t]"
    if args.diff == 'c':
        print "Start to combine dictionary & thesaurus..."
        merge('MWU')
    elif diff:
        dic_dl = dic_downloader(diff, 'MWU')
        dir = ''.join([dic_dl.DIC_T, path.sep, dic_dl.ref, path.sep])
        if not path.exists(fullpath(dic_dl.DIC_T)):
            os.mkdir(dic_dl.DIC_T)
        if diff != 't':
            imgpath = fullpath(''.join([dic_dl.DIC_T, path.sep, diff, 'p', path.sep]))
            if not path.exists(imgpath):
                os.mkdir(imgpath)
        if args.diff == 'f':
            if is_complete(fullpath(dir)):
                dic_dl.combinefiles(dir)
            else:
                print "Word-downloading is not completed."
        else:
            dic_dl.login()
            if dic_dl.session:
                print "Start to make %s..." % dic_dl.ref
                multiprocess_fetcher(dic_dl, dir)
                if is_complete(fullpath(dir)):
                    dic_dl.combinefiles(dir)
                print "Done!"
                dic_dl.logout()
            else:
                print "ERROR: Login failed."
    print "Finished at %s" % datetime.now()
