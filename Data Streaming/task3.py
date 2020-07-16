import collections
import csv
import random
import sys

import tweepy

API_KEY = "p7YaUMVKwyVpYxOJsB0Ubzltr"
API_SECRET = "56JxQ1Ql3J0XZMsA9HVnVJWA7y5SSM2lVmmdcFhE8wNZRnNKVv"
ACCESS_TOKEN = "960865232959692800-VcEEbf66ycOPWRnz5yUHmPJb8bgpvY5"
ACCESS_TOKEN_SECRET = "a8jaDpTxSXku7CtZnYNQ1XkqS0XmbMCJamYBLtz6XAMqY"

topics = ['COVID19', 'SocialDistancing', 'StayAtHome', 'Trump',
              'Quarantine', 'CoronaVirus', 'China', 'Wuhan', 'Pandemic']


class TwitterStream(tweepy.StreamListener):

    def __init__(self, export_file_path):
        tweepy.StreamListener.__init__(self)
        self.saved_tweets = collections.defaultdict(list)
        self.sequence_number = 0
        self.existed_tags = collections.defaultdict(lambda: 0)

        self.export_file_path = export_file_path
        self._result_file()

        
        self.removeable_index = set()

    def on_status(self, status):
        tag_dict_list = status.entities.get('hashtags')

        if self.sequence_number <= 100:

            self._initial_data(tag_dict_list)
            self.sequence_number += 1
        else:
            probability = float(100.0 / self.sequence_number)

            if calProb(probability):
                updated_flag = self._update(tag_dict_list)

                if updated_flag:
                    self._export_result()
                    self.sequence_number += 1

    def _initial_data(self, dict_list):
        temp_list = list()
        if len(dict_list) > 0:
            for dict_item in dict_list:
                tag = dict_item.get('text')
                self.existed_tags[tag] += 1
                temp_list.append(tag)
        else:
            self.removeable_index.add(self.sequence_number)

        self.saved_tweets[self.sequence_number] = temp_list

    def _result_file(self):
        with open(self.export_file_path, "w") as output_file:
            csv.writer(output_file)

    def _update(self, dict_list):
        temp_list = list()
        if len(dict_list) > 0:

            position = self._get_one_position()

            for dict_item in dict_list:
                tag = dict_item.get('text')
                self.existed_tags[tag] += 1
                temp_list.append(tag)

            if len(self.saved_tweets[position]) > 0:
                need_2_remove_tags = self.saved_tweets[position]
                for tag in need_2_remove_tags:
                    self.existed_tags[tag] -= 1
                    if self.existed_tags[tag] == 0:
                        self.existed_tags.pop(tag)

            self.saved_tweets[position] = temp_list
            return True

        return False

    def _get_one_position(self):
        if len(self.removeable_index) > 0:
            position = random.choice(list(self.removeable_index))
            self.removeable_index.discard(position)
            return position
        else:
            return random.randint(0, 99)

    def _export_result(self):
        sorted_tag_dict = sorted(self.existed_tags.items(), key=lambda kv: (-kv[1], kv[0]))
        top3_val = sorted(set(self.existed_tags.values()), reverse=True)[:3]
        outfile = open(self.export_file_path, "a", encoding="utf-8")
        outfile.write("The number of tweets with tags from the beginning: {}\n".format(self.sequence_number))
        for key, value in sorted_tag_dict:
            if value in top3_val:
                outfile.write(key + " : " + str(value) + "\n")

        outfile.write("\n")
        outfile.close()


def calProb(probability):
    return random.random() < probability


if __name__ == '__main__':
    output_file_path = "../out/task3.csv"
    # output_file_path = sys.argv[2]
    streamListener = TwitterStream(export_file_path=output_file_path)
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    stream = tweepy.Stream(auth=auth, listener=streamListener)
    stream.filter(track=topics, languages=["en"])