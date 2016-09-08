#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
@author dingwieyue@baidu.com
@brief 游戏兴趣挖掘匹配、得到分类 output "cuid matched_query(cate_id)|pv|date" 

"""
import os
import sys
import ahocorasick
import codecs
import task_base

#INPUTFILE_PATH = os.path.abspath('%s/../' %__file__)
INPUTFILE_PATH = "./game_dict"
sys.path.append(os.path.abspath('%s/../../' % __file__))

reload(sys)
sys.setdefaultencoding("utf-8")

interest_noun_dict_path = INPUTFILE_PATH + "/game_interest_prop_noun"
score_dict_file = INPUTFILE_PATH + "/game_cate_score"

class InterestGameTask(task_base.TaskBase):
    """Game task
        加载游戏专名词典
    """
    def __init__(self):
        super(self.__class__, self).__init__(task_base.ALL_TASK_NAME.interest_game)
        self.__interest_tree_noun = ahocorasick.KeywordTree()
        with codecs.open(interest_noun_dict_path, mode='r', encoding='UTF-8') as fr:
            for line in fr:
                line = line.strip()
                if len(line) >= 2:
                    self.__interest_tree_noun.add(line)
        self.__interest_tree_noun.make()
        self.__init_Getscore(score_dict_file)
        self.cuid_score = {}

    def __init_Getscore(self, score_dict_file):
        self.score_dict = {}
        with open(score_dict_file) as f:
            for line in f:
                cols = line.strip().split('\t')
                name = cols[0]
                if not name or (len(cols)<2):
                    continue
                if name not in self.score_dict:
                    self.score_dict[name] = {}
                cate_score = cols[1].split('|')
                for i in cate_score:
                    eles = i.split(':')
                    if eles[0]:
                        self.score_dict[name][eles[0]] = eles[1]
        return self.score_dict

    def process(self, cuid, query_word, date):
        """
        处理单行
        """
        query_word = query_word.encode('UTF-8')
        matched_arr = []
        for match in self.__interest_tree_noun.findall(query_word, allow_overlaps=0):
            matched_word = query_word[match[0]:match[1]]
            matched_arr.append(matched_word)
        if cuid not in self.cuid_score:
            self.cuid_score[cuid] = {}
        for i in matched_arr:
            if i not in self.score_dict:
                continue
            catescore_dict = self.score_dict[i]
            word_cateid = self.maxscore_cateid(catescore_dict, i)
            if word_cateid not in self.cuid_score[cuid]:
                self.cuid_score[cuid][word_cateid] = {}
                self.cuid_score[cuid][word_cateid][date] = 0
            else:
                if date not in self.cuid_score[cuid][word_cateid]:
                    self.cuid_score[cuid][word_cateid][date] = 0
            self.cuid_score[cuid][word_cateid][date] += 1
        return self.cuid_score

    def maxscore_cateid(self, scoredict, i):
        """
        选择score最大的子类作为其小类的分类
        """
        tmp = sorted(scoredict.items(), key=lambda x:float(x[1]), reverse=True)
        return 'Term_' + i + '(' + '1900' + tmp[0][0].strip() + ')'

    def cmb_keyvalue_dict(self, dictfile):
        """
        输出 term_word|pv|date
        """
        dict_items = dictfile.items()
        string = ''
        for i in dict_items: 
            if isinstance(i[1], dict): 
                string = i[0] + '|' + self.cmb_keyvalue_dict(i[1]) + '\t' + string
            else:
                string = str(i[1]) + '|' + str(i[0]) + '\t' + string
        return string
        
    def print_out(self):
        """
        输出
        """
        for cuid in self.cuid_score:
            if self.cmb_keyvalue_dict(self.cuid_score[cuid]):
                w = cuid + '\tbaiduinput\t' + self.cmb_keyvalue_dict(self.cuid_score[cuid])
                if len(w.split('\t'))>2:
                    print w

if __name__ == '__main__':
    # 测试用
    game_task = InterestGameTask()
    for line in sys.stdin:
        """
            input
            cuid time query app
            0    1     2     3
        """
        eles = line.strip().split("\t")
        if len(eles) != 4:
            continue
        cuid = eles[0]
        time = eles[1]
        import datetime
        date = datetime.datetime.fromtimestamp(float(time)).strftime('%Y%m%d%H')
        query_word = eles[2]

        predict_result = game_task.process(cuid, query_word, date)
        game_task.print_out()
        
