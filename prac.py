import json
from allennlp.modules.token_embedders.bert_token_embedder import PretrainedBertEmbedder
from allennlp.data.token_indexers.wordpiece_indexer import PretrainedBertIndexer
from allennlp.data.vocabulary import Vocabulary
from allennlp.data.fields import ListField, LabelField, ArrayField
from allennlp.nn import util
from allennlp.data.instance import Instance
from allennlp.data.iterators import BucketIterator
from extractor_model import TriggerExtractor, ArgumentExtractor
from extractor_metric import ExtractorMetric
from dueereader import DataMeta, TriggerReader, RoleReader, ETRoleReader, ETListReader

from allennlp.data.dataset_readers import DatasetReader


class ListReader(DatasetReader):
    def __init__(self, data_meta, token_indexer):
        super().__init__()
        self.token_indexer = token_indexer
        self.data_meta = data_meta
        self.wordpiece_tokenizer = token_indexer['tokens'].wordpiece_tokenizer

    def str_to_instance(self, line):
        line = json.loads(line)

        words = line['text']
        words_field = MetadataField(words)

        tokens = [Token(word) for word in words]
        sentence_field = TextField(tokens, self.token_indexer)

        fields = {'sentence': sentence_field}

        fields['origin_text'] = words_field
        # print(fields)

        sentence_id = line['id']
        sentence_id_field = MetadataField(sentence_id)
        fields['sentence_id'] = sentence_id_field

        return Instance(fields)

    def _read(self, lines):
        for line in lines:
            yield self.str_to_instance(line)


bert_indexer = {'tokens': PretrainedBertIndexer(
    pretrained_model=args.bert_vocab,
    use_starting_offsets=True,
    do_lowercase=False)}

data_meta = DataMeta(event_id_file=args.data_meta_dir + "/events.id", role_id_file=args.data_meta_dir + "/roles.id")

# ==== iterator =====
vocab = Vocabulary()
iterator = BucketIterator(
    sorting_keys=[('sentence', 'num_tokens')],
    batch_size=args.extractor_batch_size)
iterator.index_with(vocab)

trigger_model_path = args.save_trigger_dir

text_reader = ListReader(data_meta=data_meta, token_indexer=bert_indexer)

with open("precessed_test.json", 'w') as pf:
    with open("tttt.json", 'r') as f:
        # data = f.readlines()
        data = json.load(f)

        for i, _line in enumerate(data):  # ?????????????????????
            full_text = _line["full_text"]
            # ???????????? sale_text ??????
            sale_list = _line["sale_text"]

            # ?????? sale_list ???????????????list
            input_list = []
            for j, _sent in enumerate(sale_list):
                id = 'call' + str(i) + '_' + 'sale' + str(j)  # e.g. call5_sale7 ????????????5?????????sale??????7??????
                temp_dict = {"id": id, "text": _sent}
                input_list.append(temp_dict)
            print(input_list)

            pre_dataset = text_reader.read(input_list)  # ????????????????????????trigger?????????
            # ??????trigger?????????????????????????????????????????????????????????????????? ?????????????????????trigger??????????????????
            instances = trigger_extractor_deal(pre_dataset=pre_dataset, iterator=iterator,
                                               trigger_model_path=trigger_model_path, dataset_meta=data_meta)
            # ????????????instance ?????????trigger???event_type???
            # ?????????????????????????????????trigger??????
            tri_sent_id = None
            tri_sent = None
            for instance in instances:
                temp_id = instance['sentence_id'].metadata
                temp_trigger = instance['trigger_id'].metadata

                sale_sent_id = int(temp_id.split('_')[4:])  # callx_salexxx ???????????? xxx
                if tri_sent_id is None or tri_sent_id < sale_sent_id:
                    tri_sent_id = sale_sent_id
                    tri_sent = instance['sentence'].metadata
            # ??? full_text ??????????????????????????????????????????????????????????????????
            text = "".join(full_text[full_text.find(tri_sent):])
            pf.write(json.dumps({"text": "xxx", "id": "call" + str(i)}, ensure_ascii=False) + '\n')
