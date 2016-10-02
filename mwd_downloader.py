#!/usr/bin/env python
# -*- coding: utf-8 -*-
## mwd_downloader.py
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
import urllib
import random
import string
import shutil
import fileinput
import requests
from os import path
from datetime import datetime
from multiprocessing import Pool
from collections import OrderedDict


MAX_PROCESS = 5
STEP = 8000
F_WORDLIST = 'wordlist.txt'
ORIGIN = 'http://www.merriam-webster.com/'
_DEBUG_ = 1


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


def randomstr(digit):
    return ''.join(random.sample(string.ascii_letters, 1)+
        random.sample(string.ascii_letters+string.digits, digit-1))


def info(l, s='word'):
    return '%d %ss' % (l, s) if l>1 else '%d %s' % (l, s)


def fix_c(c):
    return c.replace('%', '%25').replace('/', '%2F').replace('&', '%26').replace('\'', '%27').replace('?', '%3F')


def getwordlist(file, base_dir='', tolower=False):
    words = readdata(file, base_dir)
    if words:
        wordlist = []
        p = re.compile(r'\s*\n\s*')
        words = p.sub('\n', words).strip()
        for word in words.split('\n'):
            try:
                w, u = word.split('\t')
                if tolower:
                    wordlist.append((w.strip().lower(), u.strip().lower()))
                else:
                    wordlist.append((w, u))
            except Exception, e:
                import traceback
                print traceback.print_exc()
                print word
        return wordlist
    print("%s: No such file or file content is empty." % file)
    return []


def getpage(link, BASE_URL=''):
    r = requests.get(''.join([BASE_URL, link]), timeout=10, allow_redirects=False)
    if r.status_code == 200:
        return r.content
    else:
        return None


def getpage2(link, BASE_URL=''):
    url = ''.join([BASE_URL, link])
    r = requests.get(url, timeout=10)
    if r.history:
        rurl = urllib.unquote(str(r.url))
        if re.compile(r'#\w+Dictionary\s*$', re.I).search(rurl) or\
            not re.compile(r'/dictionary/[^<>/]+$', re.I).search(rurl):
            return 200, ' '
        elif urllib.unquote(url).lower() != rurl.lower():
            ref = re.compile(r'/dictionary/([^<>/]+)\s*$').search(rurl).group(1)
            return 301, ref
    if r.status_code == 200:
        return 200, r.content
    else:
        return 404, None


class downloader:
#common logic
    def __init__(self, name):
        self.__session = None
        self.DIC_T = name

    @property
    def session(self):
        return self.__session

    def login(self, REF=''):
        HEADER = 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'
        self.__session = requests.Session()
        self.__session.headers['User-Agent'] = HEADER
        self.__session.headers['Origin'] = ORIGIN
        self.__session.headers['Referer'] = REF

    def __mod(self, flag):
        return 'a' if flag else 'w'

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

    def __fetchdata_and_make_mdx(self, arg, part, suffix=''):
        sdir, d_app, d_w = arg['dir'], OrderedDict(), OrderedDict(part)
        words, crefs, count, logs, failed = [], OrderedDict(), 1, [], []
        leni = len(part)
        while leni:
            for url, cur in part:
                if count % 100 == 0:
                    print ".",
                    if count % 1000 == 0:
                        print count,
                try:
                    status, page = getpage2(self.makeurl(url))
                    if page:
                        if status == 200:
                            if self.makeword(page, cur, words, logs, d_app):
                                crefs[cur] = url
                                count += 1
                        else:
                            assert cur.strip().lower()!=page.strip().lower()
                            words.append((cur, ''.join(['@@@LINK=', page])))
                    else:
                        failed.append((url, cur))
                except requests.TooManyRedirects, e:
                    logs.append("E01:\tCan't download '%s', TooManyRedirects" % cur)
                    print url
                except Exception, e:
                    import traceback
                    print traceback.print_exc()
                    print "%s failed, retry automatically later" % cur
                    failed.append((url, cur))
            lenr = len(failed)
            if lenr >= leni:
                break
            else:
                leni = lenr
                part, failed = failed, []
        print "%s browsed" % info(count-1),
        if crefs:
            mod = self.__mod(path.exists(fullpath('cref.txt', base_dir=sdir)))
            dump(''.join(['\n'.join(['\t'.join([k, v]) for k, v in crefs.iteritems()]), '\n']), ''.join([sdir, 'cref.txt']), mod)
        d_app2 = OrderedDict()
        for k in d_app.keys():
            if not k in d_w:
                d_app2[k] = d_app[k]
        if d_app2:
            mod = self.__mod(path.exists(fullpath('appd.txt', base_dir=sdir)))
            dump(''.join(['\n'.join(['\t'.join([k, v]) for k, v in d_app2.iteritems()]), '\n']), ''.join([sdir, 'appd.txt']), mod)
        if failed:
            dump(''.join(['\n'.join(['\t'.join([w, u]) for w, u in failed]), '\n']), ''.join([sdir, 'failed.txt']))
            self.__dumpwords(sdir, words, '.part', False)
        else:
            print ", 0 word failed"
            self.__dumpwords(sdir, words, suffix)
        if logs:
            mod = self.__mod(path.exists(fullpath('log.txt', base_dir=sdir)))
            dump('\n'.join(logs), ''.join([sdir, 'log.txt']), mod)
        return len(crefs), d_app2

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
            failed = getwordlist('failed.txt', sdir)
            return self.__fetchdata_and_make_mdx(arg, failed, '.part')
        elif not path.exists(fp3):
            print ("New session started")
            return self.__fetchdata_and_make_mdx(arg, arg['alp'])

    def getcreflist(self, file, base_dir=''):
        words = readdata(file, base_dir)
        if words:
            p = re.compile(r'\s*\n\s*')
            words = p.sub('\n', words).strip()
            crefs = OrderedDict()
            for word in words.split('\n'):
                k, v = word.split('\t')
                crefs[k.lower()] = k
                crefs[v.lower()] = k
            return crefs
        print("%s: No such file or file content is empty." % file)
        return OrderedDict()

    def combinefiles(self, dir):
        times = 0
        for d in os.listdir(fullpath(dir)):
            if re.compile(r'^\d+$').search(d) and path.isdir(fullpath(''.join([dir, d, path.sep]))):
                times += 1
        dtp = ''.join([dir, 'data', path.sep])
        for imgdir in [fullpath(dtp), fullpath(''.join([dtp, 'p'])), fullpath(''.join([dtp, 'v']))]:
            if not path.exists(imgdir):
                os.mkdir(imgdir)
        print "combining files..."
        for fn in ['cref.txt', 'log.txt']:
            fw = open(fullpath(''.join([dir, fn])), 'w')
            for i in xrange(1, times+1):
                sdir = ''.join([dir, '%d'%i, path.sep])
                if path.exists(fullpath(fn, base_dir=sdir)):
                    fw.write('\n'.join([readdata(fn, sdir).strip(), '']))
            fw.close()
        words, logs, buf = [], [], []
        self.set_repcls()
        self.crefs = self.getcreflist('cref.txt', dir)
        self.clstbl = OrderedDict()
        self.need_fix = OrderedDict()
        dics = [formatter((self, ''.join([dir, '%d'%1, path.sep])))]
        pool, params = Pool(5), []
        for i in xrange(2, times+1):
            params.append((self, ''.join([dir, '%d'%i, path.sep])))
        dics.extend(pool.map(formatter, params))
        for dic, word in dics:
            words.extend(word)
            self.crefs.update(dic.crefs)
            if _DEBUG_:
                self.clstbl.update(dic.clstbl)
                self.need_fix.update(dic.need_fix)
        file = ''.join([dir, self.DIC_T, path.extsep, 'txt'])
        dump('', file)
        for i in xrange(1, times+1):
            sdir = ''.join([dir, '%d'%i, path.sep])
            text = readdata('formatted.txt', base_dir=sdir)
            if text:
                dump(self.fix_links(text), file, 'a')
                os.remove(fullpath('formatted.txt', base_dir=sdir))
        print "%s totally" % info(len(words))
        dump('\n'.join(words), ''.join([dir, 'words.txt']))
        if logs:
            mod = self.__mod(path.exists(fullpath('log.txt', base_dir=dir)))
            dump('\n'.join(logs), ''.join([dir, 'log.txt']), mod)
        if _DEBUG_:
            if self.clstbl:
                del buf[:]
                for k, v in self.clstbl.iteritems():
                    buf.append(k)
                    for c, r in v.iteritems():
                        buf.append(''.join([' \'', c, '\': \'', r, '\',']))
                dump('\n'.join(buf), ''.join([dir, 'cls.txt']))
            if self.need_fix:
                dump(''.join(['\n'.join(['\n'.join([k, v, '</>']) for k, v in self.need_fix.iteritems()]), '\n']), ''.join([dir, 'check_cls.txt']))
                dump(''.join(['\n'.join(['\t'.join([k.replace(' ', '_'), k]) for k, v in self.need_fix.iteritems()]), '\n']), ''.join([dir, 'wordlist.txt']))


def f_start((obj, arg)):
    return obj.start(arg)


def formatter((dic, sdir)):
    words, logs, fmtd = [], [], []
    file = ''.join([sdir, 'formatted.txt'])
    fmtd = dic.load_file(sdir, words, logs)
    if fmtd:
        dump(''.join(fmtd), file)
    print sdir
    return dic, words


def multiprocess_fetcher(dir, d_refs, wordlist, obj, base):
    times = int(len(wordlist)/STEP)
    pl = [wordlist[i*STEP: (i+1)*STEP] for i in xrange(0, times)]
    pl.append(wordlist[times*STEP:])
    times = len(pl)
    fdir = fullpath(dir)
    if not path.exists(fdir):
        os.mkdir(fdir)
    for i in xrange(1, times+1):
        subdir = ''.join([dir, '%d'%(base+i)])
        subpath = fullpath(subdir)
        if not path.exists(subpath):
            os.mkdir(subpath)
    for dest in 'pv':
        imgdir = fullpath(''.join([dir, dest]))
        if not path.exists(imgdir):
            os.mkdir(imgdir)
    pool, n = Pool(MAX_PROCESS), 1
    d_app = OrderedDict()
    while n:
        args = []
        for i in xrange(1, times+1):
            sdir = ''.join([dir, '%d'%(base+i), path.sep])
            file = fullpath(sdir, 'rawhtml.txt')
            if not(path.exists(file) and os.stat(file).st_size):
                param = {}
                param['alp'] = pl[i-1]
                param['dir'] = sdir
                args.append((obj, param))
        if len(args) > 0:
            vct = pool.map(f_start, args)#[f_start(args[0])]#for debug
            n = 0
            for count, dt in vct:
                n += count
                d_app.update(dt)
        else:
            break
    dt = OrderedDict()
    for k, v in d_app.iteritems():
        if not k in d_refs:
            dt[k] = v
    return times, dt.items()


class mwd_downloader(downloader):
#mwd downloader
    def __init__(self):
        downloader.__init__(self, 'MWD')
        self.__base_url = ORIGIN
        self.__re_d = {re.I: {}, 0: {}}

    def makeurl(self, cur):
        return ''.join([self.__base_url, 'dictionary/', cur])

    def __rex(self, ptn, mode=0):
        if ptn in self.__re_d[mode]:
            pass
        else:
            self.__re_d[mode][ptn] = re.compile(ptn, mode) if mode else re.compile(ptn)
        return self.__re_d[mode][ptn]

    def __repcls(self, m):
        tag = m.group(1)
        cls = m.group(3)
        if tag in self.__trs_tbl and cls in self.__trs_tbl[tag]:
            return ''.join([tag, m.group(2), self.__trs_tbl[tag][cls]])
        else:
            return m.group(0)

    def cleansp(self, html):
        p = self.__rex(r'<!--[^<>]+>')
        html = p.sub(r'', html)
        p = self.__rex(r'\s{2,}')
        html = p.sub(r' ', html)
        p = self.__rex(r'\s*<br/?>\s*')
        html = p.sub('<br>', html)
        p = self.__rex(r'(\s*<br>\s*)*(<(?:/?(?:div|p|ul|ol|li|hr)[^>]*|br)>)(\s*<br>\s*)*', re.I)
        html = p.sub(r'\2', html)
        p = self.__rex(r'(\s*<br>\s*)*(<(?:/?(?:div|p)[^>]*|br)>)(\s*<br>\s*)*', re.I)
        html = p.sub(r'\2', html)
        p = self.__rex(r'(?:\s|&#160;)*(<(?:/?(?:div|p|ul|ol|li|table|tr)[^>]*|br)>)(?:\s|&#160;)*', re.I)
        html = p.sub(r'\1', html)
        p = self.__rex(r'(?<=[^,])\s+(?=[,;\?]|\.(?:[^\d\.]|$))')
        html = p.sub(r'', html)
        return html

    def __preformat(self, page):
        page = page.replace('\xC2\xA0', ' ')
        p = self.__rex(r'[\n\r]+(\s+[\n\r]+)?')
        page = p.sub(' ', page)
        n = 1
        while n:
            p = self.__rex(r'\t+|&(?:nb|en|em|thin)sp;|\s{2,}')
            page, n = p.subn(r' ', page)
        p = self.__rex(r'(</?)strong(?=[^>]*>)')
        page = p.sub(r'\1b', page)
        return page

    def __get_img(self, div, dest='p'):
        p = self.__rex(r'<img\b[^<>]*src="([^<>"]+?/([^<>"/]+))"', re.I)
        for url, file in p.findall(div):
            self.__dl_img(url, dest, file)

    def __dl_img(self, url, dest, file):
        file = ''.join([self.DIC_T, path.sep, dest, path.sep, file])
        fp = fullpath(file)
        if not path.exists(fp) or not path.getsize(fp):
            img = getpage(url)
            dump(img, file, 'wb')

    def makeword(self, page, word, words, logs, d_app):
        exist = False
        page = self.__preformat(page)
        p = self.__rex(r'<article>(.+?)</article>', re.I)
        m = p.search(page)
        if m:
            worddef = m.group(1)
            p = re.compile(r'(?<=<img )[^<>]*?src="/(math/)([^<>/"]+)"[^<>]*(?=>)', re.I)
            for link, file in p.findall(worddef):
                self.__dl_img(''.join([ORIGIN, link, file]), 'v', file)
            pi = self.__rex(r'<div class="inner-box-wrapper">\s*(.+?</div>)\s*</div>', re.I)
            p = self.__rex(r'<a class="table-link" href="/([^<>"]+)">([^<>]+)</a>', re.I)
            for u, w in p.findall(worddef):
                ap = self.__preformat(getpage(u, ORIGIN))
                for tbl in pi.findall(ap):
                    words.append([w, ''.join(['<TBL>', tbl, '</TBL>'])])
                    self.__get_img(tbl, 'v')
            p = self.__rex(r'<a [^<>]*href="/([^<>"]+)"[^<>]*>\s*<img [^<>]*>\s*</a>', re.I)
            for a in p.findall(worddef):
                ap = self.__preformat(getpage(a, ORIGIN))
                for im in pi.findall(ap):
                    worddef = ''.join([worddef, '<ILLU>', im, '</ILLU>'])
                    self.__get_img(im)
            pop = self.__preformat(getpage(''.join(['http://stats.merriam-webster.com/pop-score-redesign.php?word=', urllib.quote(word)])))
            worddef = ''.join([worddef, '<POP>', pop, '</POP>'])
            words.append([word, worddef])
            exist = True
        else:
            logs.append("I01:\t'%s' is not found in MWD" % word)
        return exist

    def load_file(self, sdir, words, logs):
        file, buf = fullpath('rawhtml.txt', base_dir=sdir), []
        if not path.exists(file):
            print "%s does not exist" % file
            return []
        lns = []
        for ln in fileinput.input(file):
            ln = ln.strip()
            if ln == '</>':
                text = self.format(lns[0], lns[1], logs)
                if text:
                    buf.append(text)
                    words.append(lns[0])
                del lns[:]
            elif ln:
                lns.append(ln)
        return buf

    def set_repcls(self):
        self.__trs_tbl = {'div': {'uro': 'ub9', 'inner-box-wrapper': 'hfv',
        'toggle-box full-def-box def-header-box card-box def-text ': 'hbz',
        'word-attributes': 'h1n', 'card-primary-content': 'rhj', 'modules clearfix': 'anp',
        'card-box simple-def-box secondary-card usage-box usage-module-anchor': 'nvt',
        'card-box examples-box simple-def-box secondary-card verb-def-box examples-module-anchor': 'dl0',
        'card-primary-content def-text': 'ycy', 'card-box small-box origin-box': 'alt',
        'card-box small-box other-x-terms-box end': 'wog', 'card-primary-content list-block': 'kwj',
        'toggle-box full-def-box def-header-box card-box def-text another-def def-text': 'oi3',
        'toggle-box full-def-box def-header-box card-box def-text show-collapsed another-def def-text': 'hca',
        'toggle-box full-def-box def-header-box card-box def-text show-collapsed ': 'fgp',
        'toggle-box full-def-box def-header-box card-box def-text has-tw-flag': 'qs3',
        'card-box simple-def-box synonym-discussion-box end synonym-discussion-module-anchor': 'gub',
        'definition-block': 'd9d', 'link-cta-container': 'wtl', 'us': 'dss',
        'card-box small-box origin-box end': 'jyn', 'word-attributes no-fl': 'xcu',
        'word-header': 'v4a', 'word-and-pronunciation': 'pbw',
        'card-box small-box other-x-terms-box': 'opv', 'image-word-wrapper': 'ils',
        'card-box full-def-box secondary-card def-text show-collapsed': 'cpd',
        'tense-box quick-def-box simple-def-box card-box def-text ': 'yal',
        'definition-block def-text': 'q5y', 'card-box small-box related-box': 'dte',
        'card-box full-def-box secondary-card def-text ': 'npf',
        'card-primary-content linked-table': 'leo', 'related-content container clearfix': 'ngx',
        'card-box secondary-card related-phrases-box show-collapsed full-def-box': 'wdn',
        'little-gems-box card-box simple-def-box secondary-card verb-def-box little-gems-module-anchor': 'fzu',
        'card-box small-box first-use-box end': 'qsy', 'word-header long-headword': 'yix',
        'card-box small-box related-box end': 'kbm', 'card-box small-box first-use-box': 'plu',
        'card-box simple-def-box synonym-discussion-box synonym-discussion-module-anchor': 'vzp',
        'tense-box quick-def-box simple-def-box card-box def-text another-def def-text': 'srx',
        'tense-box quick-def-box simple-def-box card-box def-text has-tw-flag': 'd7f'},
        'span': {'main-attr': 'qun', 'intro-colon': 'gmb', 'inflections': 'iqw',
        'word-syllables': 'ttw', 'in': 'zf2', 'in-more': 'bo3', 'unicode': 'g5i',
        'ibar': 'crk', 'code_hrev': 'lcu', 'code_uhorn': 'ink', 'code_ohornac': 'ise',
        'ph': 'ibz', 'sm-caps': 'x7u', 'fr': 'pfu', 'code_ibartild': 'bxq', 'code_openocrc': 'vgn'},
        'ol': {'definition-list': 'lvc', 'definition-list no-count': 'jza'},
        'p': {'definition-inner-item with-sense': 'auc', 'definition-inner-item': 'ohk',
        'see-in-addition': 'vwo'},
        'em': {'sense': 'lfu', 'sub sense num': 'nqw', 'qword': 'qky', 'sc': 'j8z',
        'sub sense alp': 'pys', 'it': 'yvp'},
        'h2': {'card-box-title': 'gfr', 'typo7 margin-t-10px margin-b-30px text-center': 'y5b'},
        'ul': {'definition-list no-count': 'nih'}, 'li': {'hide-def-content': 'uy1'}}

    def __fmt_hd(self, m):
        hd = m.group(1)
        p = self.__rex(r'<(h\d)>([^<>]+)\s*</\1>', re.I)
        hd = p.sub(r'<h1><span class="noj">\2</span></h1>', hd)
        return hd

    def __fmt_h1(self, m):
        h1 = m.group(1)
        p = self.__rex(r'<span class="word-syllables">\s*([^<>]+?)\s*</span>', re.I)
        n = p.search(h1)
        if n:
            q = self.__rex(r'(?<=<span class="noj">)[^<>]+(?=</span>)', re.I)
            h1 = q.sub(n.group(1).replace('&#183;', '<em></em>'), h1)
            h1 = p.sub(r'', h1)
        return h1

    def __fmt_illu(self, illu):
        p = self.__rex(r'(</?)(?:it?|em)(?=>)', re.I)
        illu, n = p.subn(r'\1b', illu)
        p = self.__rex(r'<b>(\s*or\s*)</b>', re.I)
        illu, i = p.subn(r'<i>\1</i>', illu)
        if n%2 != 0:
            p = self.__rex(r'(?<=<p>)\s*</b>', re.I)
            illu = p.sub(r'', illu)
            p = self.__rex(r'(<p>[^<>]*)</b>([^<>]*</p>)', re.I)
            illu = p.sub(r'\1\2', illu)
            p = self.__rex(r'(?<=<b>)([^<>]+)(?=</p>)', re.I)
            illu = p.sub(r'\1</b>', illu)
            illu = self.__rex(r'(</b>)?<span class="rb">', re.I).sub(r'</b>', illu)
            illu = self.__rex(r'<span class="dn">(\d)', re.I).sub(r'<sub>\1</sub>', illu)
        cls = 'dvm' if (n-i)>12 else 'gmw'
        p = self.__rex(r'(</?)p(?=>)', re.I)
        illu = p.sub(r'\1div', illu)
        p = self.__rex(r'(?<=<img )[^<>]*(src=")http://[^<>]+?/([^<>/]+?)"[^<>]*(?=>)', re.I)
        illu = p.sub(lambda n: self.__copy_img(n, 'p'), illu)
        return ''.join(['<ILLU class="', cls, '">', illu, '</ILLU>'])

    def __copy_img(self, m, dest):
        file = m.group(2)
        of = fullpath(''.join([self.DIC_T, path.sep, dest, path.sep, file]))
        nf = fullpath(''.join([self.DIC_T, path.sep, 'data', path.sep, dest, path.sep, file]))
        if not path.exists(nf):
            shutil.copyfile(of, nf)
        if dest == 'v':
            file, ext = path.splitext(file)
            file = ''.join([file, '.png'])
        return ''.join(['src="', dest, '/', file, '"'])

    def __fmt_pos(self, m):
        pos = m.group(2)
        if pos.find(' name')>-1 or pos.find('often')>-1:
            return m.group(0)
        return ''.join([m.group(1), ' class="ozr"', pos])

    def __fmt_tt(self, m):
        lb, tt = m.group(2), m.group(3).replace('Origin and Etymology', 'Origin')
        tt = ''.join([tt[:1], tt[1:].lower()])
        return ''.join(['<h2', lb, '<span class="tih">', tt, '</span><img src="c.png" class="nri" onclick="mwz.x(this)"></h2>'])

    def __fmt_qt(self, exm):
        exm = self.__rex(r'<em>([^<>]+)</em>', re.I).sub(r'<i>\1</i>', exm)
        pos = exm.rfind('\xE2\x80\x94')
        if pos > -1:
            exm = ''.join(['<q>', exm[:pos], '</q><cite>', exm[pos:], '</cite>'])
        else:
            exm = ''.join(['<q>', exm, '</q>'])
        return exm

    def __fmt_exm(self, m):
        exm = ''.join([m.group(1), m.group(2)])
        exm = self.__fmt_qt(exm)
        return ''.join(['<span class="jyi">', exm, '</span>'])

    def __fmt_exm2(self, m):
        exm = m.group(1)
        p = self.__rex(r'(?<=<p class=")definition-inner-item(">)&lt;(.+?)&gt;\s*((?:[\.,\?\!])?)(?=</p>)', re.I)
        exm = p.sub(lambda n: ''.join(['yx4', n.group(1), self.__fmt_qt(''.join([n.group(2), n.group(3)]))]), exm)
        p = self.__rex(r'(?<=<p class="definition-inner-item">)(.+?)(?=</p>)', re.I)
        exm = p.sub(lambda n: self.__fmt_qt(n.group(1)), exm)
        return exm

    def __fmt_pron(self, m):
        pr = m.group(2).replace('\x6F\xCC\x87', '<span class="ixo">\x6F\xCC\x87</span>')
        return ''.join(['uig', m.group(1), '/', pr, '/'])

    def __fmt_sym(self, m):
        sym = m.group(1)
        p = self.__rex(r'<(h\d)>(Synonyms)</\1>', re.I)
        if sym.count('</h6>') > 1:
            sym = p.sub(r'<div class="oaf">\2</div>', sym)
            p = self.__rex(r'<(h\d)>(Antonyms)</\1>', re.I)
            sym = p.sub(r'<div class="ugp">\2</div>', sym)
        else:
            sym = p.sub(r'', sym)
        return sym

    def __reg_drv(self, m):
        uk = m.group(2).strip().lower()
        if not uk in self.crefs:
            self.crefs[uk] = self.key
        return ''.join(['<span class="bnu">', m.group(2), '</span> '])

    def __fmt_drv(self, m):
        drv = m.group(1)
        p = self.__rex(r'<(h\d)>([^<>]+)</\1>', re.I)
        drv = p.sub(self.__reg_drv, drv)
        p = self.__rex(r'((?:\s*<img\b[^<>]+>)+)\s*(<span class="uig">[^<>]+?)\s*(/</span>)', re.I)
        drv = p.sub(r' \2 \1\3 ', drv)
        p = self.__rex(r'(?<=/</span>)(?=\s*<em)', re.I)
        drv = p.sub(r',', drv)
        p = self.__rex(r'(<span class="in(?:-more)?">.+?)\s*(</span>)(?=\s*<span class="in(?:-more)?">)', re.I)
        drv = p.sub(r'\1\2, ', drv)
        return drv

    def __clr_var(self, m):
        var = m.group(1)
        p = self.__rex(r'<div class="card-primary-content">(.+?)</div>', re.I)
        var = p.sub(r'', var)
        p = self.__rex(r'<div class="inner-box-wrapper">(.+?)</div>', re.I)
        var = p.sub(r'', var)
        return var

    def __get_var(self, div):
        p = self.__rex(r'<div class="card-primary-content">(.+?)</div>', re.I)
        n = p.search(div)
        if n:
            return ''.join(['<div class="scv">', self.__fmt_drv(n), '</div>'])
        return ''

    def __fmt_lk(self, m):
        lb, ref, word, ed = ''.join([m.group(1), m.group(3)]), m.group(2), m.group(4), m.group(5)
        if lb.find('class="table-link"') > -1:
            cls = ' class="rlk"'
        elif lb.find('class="d_link"') > -1:
            cls = ' class="fea"'
        else:
            cls = ''
        ref = self.__rex(r'^/dictionary/|\[\d+\]$').sub(r'', ref).strip()
        if ref.lower() in self.crefs:
            ref = self.crefs[ref.lower()]
        elif word.lower() in self.crefs:
            ref = self.crefs[word.lower()]
        else:
            return ''.join(['<span class="xol">', word, '</span>', ed])
        return ''.join(['<a href="entry://', fix_c(ref), '"', cls, '>', word, '</a>', ed])

    def fix_links(self, text):
        p = self.__rex(r'<a ([^<>]*)href="([^<>]+?)"([^<>]*)>(.+?)([,\.\?]?\s*)</a>', re.I)
        text = p.sub(self.__fmt_lk, text)
        return text

    def format(self, key, line, logs):
        if line.startswith('@@@'):
            ref = line.split('=')[1]
            if ref.lower() in self.crefs:
                return '\n'.join([key, line, '</>\n'])
            else:
                logs.append("W01:\t'%s' - no such word in MWD" % ref)
                return ''
        self.key = key
        p = self.__rex(r'<TBL>\s*<div\b[^<>]*>\s*(.+?)\s*</div>\s*</TBL>')
        line, n = p.subn(r'\1', line)
        if n:
            p = self.__rex(r'(?<=<img )[^<>]*(src=")[^<>]+?/([^<>/"]+)"[^<>]*(?=>)', re.I)
            line = p.sub(lambda m: ''.join([self.__copy_img(m, 'v'), ' style="margin-left:1em;max-width:90%"']), line)
            self.crefs[key] = key
            return '\n'.join([key, ''.join(['<div style="margin-bottom:1em;color:maroon;font-family:Georgia,Times;font-size:120%"><b>', key, '</b></div>', line]), '</>\n'])
        p = self.__rex(r'<POP>\s*(.+?)\s*</POP>')
        m =p.search(line)
        if m:
            q = self.__rex(r'label:\s*([\'"])([^<>]+?)\1', re.I)
            tt = q.search(m.group(1)).group(2)
            q = self.__rex(r'img:\s*([\'"])[^<>]+?/([^<>/]+)\1', re.I)
            src = q.search(m.group(1)).group(2)
            line = p.sub(r'', line)
            line = ''.join([r'<img title="', tt, '" src="', src.replace('.gif', '.png'), '" class="kzo">', line])
        line = self.__rex(r'<div class="popularity-block">\s*</div>', re.I).sub(r'', line)
        p = self.__rex(r'<ILLU>\s*<div\b[^<>]*>\s*(.+?)\s*</div>\s*</ILLU>')
        m = p.search(line)
        illu = self.__fmt_illu(m.group(1)) if m else ''
        line = p.sub(r'', line)
        p = self.__rex(r'<div class="(?:card-box secondary-card word-by-word-box|card-box secondary-box w3-note-box)">.+?(?=<div class="(?:card-box|central-abl-box|definitions-center-creative-cont))', re.I)
        line = p.sub(r'', line)
        line = self.__rex(r'<!--[^<>]+>').sub(r'', line)
        line = self.__rex(r'<div class="clearfix">\s*</div>', re.I).sub(r'', line)
        line = self.__rex(r'<div class="simple-def-source">[^<>]+</div>', re.I).sub(r'', line)
        line = self.__rex(r'<div class="link-cta-container">.+?</div>', re.I).sub(r'', line)
        p = self.__rex(r'<a name="[^<>]+">\s*</a>\s*<h2 class="typo7', re.I)
        m = p.search(line)
        if m:
            line = line[:m.start()]
        p = self.__rex(r'<h2 class="typo7[^<>]*">.+?</h2>', re.I)
        line = p.sub(r'', line)
        line = self.__rex(r'<div class="(?:social-sidebar|wgt-related-to)[^<>]+>.+?</div>(?=\s*<script)|<script\b[^<>]*>.+?</script>', re.I).sub(r'', line)
        p = self.__rex(r'<div class="(?:card-box simple-def-box secondary-card also-found-in-card|seen-and-heard-block)"[^<>]*>.+?(?=<hr|$)', re.I)
        line = p.sub(r'', line)
        p = self.__rex(r'<div>\s*See more at <a href="/thesaurus/[^<>]*?">[^<>]+?</a>\s*</div>', re.I)
        line = p.sub(r'', line)
        p = self.__rex(r'(?<=<div class="definition-block">)(.+?)(?=</div>)', re.I)
        line = p.sub(self.__fmt_sym, line)
        p = self.__rex(r'<div class="(?:large|small|medium)-corner">.+?</div>', re.I)
        line = p.sub(r'', line)
        p = self.__rex(r'<style>\s*\.image-word-wrapper[^<>]+</style>\s*<img\b[^<>]+>', re.I)
        line = p.sub(r'', line)
        line = self.__rex(r'<hr\b[^<>]*>', re.I).sub(r'', line)
        p = self.__rex(r'<(h\d) class="card-box-title">\s*Usage\b.+?</\1>\s*<div class="card-primary-content">\s*<p>.+?</p>\s*</div>', re.I)
        m = p.search(line)
        if m:
            pt1, pt2 = line[:m.end()], line[m.end():]
            pt2 = pt2.replace(m.group(0), '')
            line = ''.join([pt1, pt2])
        p = self.__rex(r'<span class="def-num[^<>]*">(\d+)</span>\s*(<(h\d)>)([^<>]+)\s*</\3>', re.I)
        line = p.sub(r'<h1><span class="noj">\4</span> <sup>\1</sup></h1>', line)
        p = self.__rex(r'(<div class="word-header[^<>]*">.+?)(?=<div class="word-attributes[^<>]*">)', re.I)
        line = p.sub(self.__fmt_hd, line)
        p = self.__rex(r'<a [^<>]*data-file="([^<>"]+)"[^<>]*>[^<>]*<span class="play-box">\s*</span></a>', re.I)
        line = p.sub(''.join([r' <img src="sp.png" class="nsv" onclick="mwz.v(this,', '\'', r'\1', '\')">']), line)
        p = self.__rex(r'<a [^<>]*data-file="\s*"[^<>]*>[^<>]*<span class="play-box">\s*</span></a>', re.I)
        line = p.sub(r'', line)
        p = self.__rex(r'(?<=<span class=")pr(">)\\(.+?)\\(?=</span>)', re.I)
        line = p.sub(self.__fmt_pron, line)
        p = self.__rex(r'<span class="pr">(\s*<span class="uig">(.+?)</span>\s*)</span>', re.I)
        line = p.sub(r'\1', line)
        p = self.__rex(r'(</h1>)((?:\s*<img\b[^<>]+>)+)', re.I)
        line = p.sub(r'\2\1', line)
        p = self.__rex(r'<(h\d)>\s*((?:Full )?Definitions?) of(?:.+?)?</\1>', re.I)
        line = p.sub(r'<div class="das">\2</div>', line)
        p = self.__rex(r'<(h\d)[^<>]*>\s*[^<>]*\bDefinitions?(?:.+?)?</\1>', re.I)
        line = p.sub(r'', line)
        p = self.__rex(r'<div\b[^<>]+>\s*<div class="title-block">\s*(<div class="das">[^<>]+</div>)\s*</div>\s*</div>', re.I)
        line = p.sub(r'\1', line)
        ln = line.split('</h1>')
        p = self.__rex(r'(<div class="card-box small-box variants-box[^<>]*?">.+?)(?=<div class="(?:card-box|central-abl-box|definitions-center-creative-cont)|$)', re.I)
        q = self.__rex(r'<div class="card-box small-box art-box[^<>"]*">\s*<div class="inner-box-wrapper">.+?</div>\s*</div>', re.I)
        r = self.__rex(r'(?=<div class="das">)', re.I)
        for i in xrange(1, len(ln)):
            m = p.search(ln[i])
            if m:
                var = self.__get_var(m.group(1))
                ln[i-1] = ''.join([ln[i-1], var])
                ln[i] = p.sub(self.__clr_var, ln[i])
            if illu and q.search(ln[i]):
                ln[i] = r.sub(illu, ln[i], 1)
                illu = ''
        line = q.sub(r'', '</h1>'.join(ln))
        if illu:
            line = r.sub(illu, line, 1)
        n = 1
        p = self.__rex(r'<div\b[^<>]*>\s*(<(h1)\b[^<>]*>.+?</\2>)\s*</div>', re.I)
        while n:
            line, n = p.subn(r'\1', line)
        p = self.__rex(r'(</h1>)\s*(<div class="word-attributes[^<>]*">.+?</div>)', re.I)
        line = p.sub(r'\2\1', line)
        p = self.__rex(r'(?<=<h1>)(.+?)(?=</h1>)', re.I)
        line = p.sub(self.__fmt_h1, line)
        p = self.__rex(r'<ILLU( class="dvm">.+?</)ILLU>')
        m = p.search(line)
        if m:
            line = p.sub(r'', line)
            q = self.__rex(r'(?=<div class="modules clearfix">|$)', re.I)
            line = q.sub(''.join(['<div', m.group(1), 'div>']), line, 1)
        line = self.__rex(r'<ILLU( class="gmw">.+?</)ILLU>').sub(r'<div\1div>', line)
        p = self.__rex(r'<h2>Did You Know\??\s*</h2>\s*<div class="card-primary-content def-text">\s*<p>(.+?)</p>\s*</div>', re.I)
        m = p.search(line)
        if m:
            line = p.sub(r'', line)
            p = self.__rex(r'(?=<div class="das">)', re.I)
            line, n = p.subn(''.join(['<div class="ptm">', m.group(1), '</div>']), line, 1)
            assert n
        p = re.compile(r'(?<=<img )[^<>]*?src="/(math/)([^<>/"]+)"[^<>]*(?=>)', re.I)
        line = p.sub(lambda m: ''.join([self.__copy_img(m, 'v'), ' class="m1g"']), line)
        p = self.__rex(r'(?<=<em)(>[^<>]+</em>\s*)<span class="intro-colon">:</span>', re.I)
        line = p.sub(r' class="wqx"\1:', line)
        p = self.__rex(r'<em class="vi">&lt;\s*&gt;</em>', re.I)
        line = p.sub(r'', line)
        p = self.__rex(r'<em class="vi">&lt;(.+?)&gt;</em>\s*((?:[\.,;\?\!])?)', re.I)
        line = p.sub(self.__fmt_exm, line)
        p = self.__rex(r'(<sup>\d+</sup>)(\w[^<>]*(?:</a>)?)', re.I)
        line = p.sub(r'\2\1', line)
        p = self.__rex(r'<(h\d)( class="card-box-title">)\s*(?:<i>[^<>]+</i>\s*)?([^<>]+)\s*(of.+?)?</\1>', re.I)
        line = p.sub(self.__fmt_tt, line)
        p = self.__rex(r'<(h\d)>\s*(Examples?) of(.+?)?</\1>', re.I)
        line = p.sub(r'<h2><span class="tih">\2</span><img src="c.png" class="nri" onclick="mwz.x(this)"></h2>', line)
        p = self.__rex(r'(?<=<div class="card-box examples-box)(.+?)(?=<div class="(?:card-box|central-abl-box|definitions-center-creative-cont)|</ol>)', re.I)
        line = p.sub(self.__fmt_exm2, line)
        p = self.__rex(r'<li class="[^<>]+">\s*<(h\d)[^<>]*>\s*([^<>]+)\s*</\1>\s*</li>', re.I)
        line = p.sub(r'<div class="pdg"><em>\2</em></div>', line)
        p = self.__rex(r'<div class="runon-attributes">(.+?)</div>', re.I)
        line = p.sub(self.__fmt_drv, line)
        p = self.__rex(r'(<span class="in(?:-more)?">.+?</span>\s*)(?=<div)', re.I)
        line = p.sub(self.__fmt_drv, line)
        p = self.__rex(r'(<span class="main-attr">\s*<em)(>[^<>]+)(?=</em>)', re.I)
        line = p.sub(self.__fmt_pos, line)
        p = self.__rex(r'(?<=<p class=")definition-inner-item with-sense(?=">\s*<em class="sense">)', re.I)
        line = p.sub(r'idb', line)
        p = self.__rex(r'([,\.\?\!]+\s*)(</em>)', re.I)
        line = p.sub(r'\2\1', line)
        p = self.__rex(r'<font class="mark">[^<>]+</font>', re.I)
        line = p.sub('<BR><span class="dia"></span> ', line)
        p = self.__rex(r'<font class="utxt">(.+?)</font>', re.I)
        line = p.sub(r'\1', line)
        n = 1
        while n:
            line, n = self.__rex(r'<(div|li|ul|p)\b[^<>]*>\s*</\1>', re.I).subn(r'', line)
        p = self.__rex(r'(?<=<)(span|div|h2|ol|ul|li|em|p)(\b[^<>]*class=")([^<>"]+)(?=")', re.I)
        line = p.sub(self.__repcls, line)
        line = self.cleansp(line)
        js = '<script type="text/javascript"src="mw.js"></script>' if line.find('onclick=')>-1 else ''
        line = ''.join(['<link rel="stylesheet"href="', self.DIC_T, '.css"type="text/css"><div class="mst">', line, js, '</div>'])
        if _DEBUG_:
            p = self.__rex(r'<([^a]|\w{2,})\b[^<>]*class="([^<>"]{4,}|[^<>"]{,2})"')
            for tag, cls in p.findall(line):
                if tag in self.clstbl:
                    if not cls in self.clstbl[tag]:
                        self.clstbl[tag][cls] = randomstr(3).lower()
                else:
                    self.clstbl[tag] = OrderedDict()
                    self.clstbl[tag][cls] = randomstr(3).lower()
            if p.search(line):
                self.need_fix[key] = line
        return '\n'.join([key, line, '</>\n'])


def getlinks(ap, dict):
    p = re.compile(r'<div class="entries">(.+?)</div>', re.I)
    q = re.compile(r'<a href="/dictionary/([^<>]+?)">\s*([^<>]+?)\s*</a>', re.I)
    for url, word in q.findall(p.search(ap).group(1)):
        dict[url.strip()] = word


def dl_page(a):
    p = re.compile(r'[\n\r]+')
    count = 0
    while count < 20:
        try:
            return p.sub(r'', getpage(a, ORIGIN))
        except Exception, e:
            count += 1
    return ''


def makewordlist(file):
    fp = fullpath(file)
    if path.exists(fp):
        dt = OrderedDict(getwordlist(file))
    else:
        print "Get word list: start at %s" % datetime.now()
        pn = re.compile(r'[\n\r]+')
        page = dl_page('browse/dictionary/a')
        p = re.compile(r'<ul class="clearfix">.+?</ul>', re.I)
        q = re.compile(r'<li\b[^<>]*>\s*<a href="/(browse/dictionary/\w+)">[^<>]+</a>\s*</li>', re.I)
        r = re.compile(r'<ul class="pagination">.+?</ul>', re.I)
        s = re.compile(r'<li class="last">\s*<a [^<>]*href="/browse/dictionary/\w+/(\d+)">.+?</a>\s*</li>', re.I)
        dt = OrderedDict()
        for a in q.findall(p.search(page).group(0)):
            print "\n", a
            ap = dl_page(a)
            m = s.search(r.search(ap).group(0))
            if m:
                last = int(m.group(1))
            else:
                last = 0
            getlinks(ap, dt)
            for i in xrange(2, last+1):
                print ".",
                ap = dl_page(''.join([a, '/', str(i)]))
                getlinks(ap, dt)
        dump(''.join(['\n'.join(['\t'.join([k, v]) for k, v in dt.iteritems()]), '\n']), file)
        print "\nGet word list: finished at %s" % datetime.now()
    print "%s totally" % info(len(dt))
    return dt


def is_complete(dir, ext='.part'):
    if path.exists(dir):
        for root, dirs, files in os.walk(dir):
            for file in files:
                if file.endswith(ext):
                    return False
        return True
    return False


if __name__=="__main__":
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    import argparse
    argpsr = argparse.ArgumentParser()
    argpsr.add_argument("diff", nargs="?", help="[p] To download missing words \n[f] format only")
    argpsr.add_argument("file", nargs="?", help="[file name] To specify additional wordlist when diff is [p]")
    args = argpsr.parse_args()
    print "Start at %s" % datetime.now()
    mwd_dl = mwd_downloader()
    dir = ''.join([mwd_dl.DIC_T, path.sep])
    if args.diff == 'f':
        if is_complete(fullpath(dir)):
            mwd_dl.combinefiles(dir)
        else:
            print "Word-downloading is not completed."
    else:
        mwd_dl.login()
        if mwd_dl.session:
            d_all, base = makewordlist(F_WORDLIST), 0
            if args.diff=='p':
                print "Start to download missing words..."
                wordlist = []
                d_p = OrderedDict(getwordlist(args.file)) if args.file and path.exists(fullpath(args.file)) else OrderedDict()
                for d in os.listdir(fullpath(dir)):
                    if re.compile(r'^\d+$').search(d) and path.isdir(fullpath(''.join([dir, d, path.sep]))):
                        base += 1
                for k, v in d_p.iteritems():
                    if k in d_all:
                        del d_p[k]
                    else:
                        wordlist.append((k, v))
                d_all.update(d_p)
            else:
                wordlist, d_p = d_all.items(), OrderedDict()
            if wordlist:
                multiprocess_fetcher(dir, d_all, wordlist, mwd_dl, base)
                if is_complete(fullpath(dir)):
                    mwd_dl.combinefiles(dir)
            print "Done!"
        else:
            print "ERROR: Login failed."
    print "Finished at %s" % datetime.now()
