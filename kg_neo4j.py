from py2neo import Graph, Node, Relationship, cypher

"""
    python 安装目录下 \Lib\site-packages\py2neo
    源码 py2neo.data 的 文件中的 999行 取消注释 
    !新版本中直接添加 self._type = n[1]  获取 relation的关系
"""


class KgNeo4j(object):

    def __init__(self, url=None, usr=None, pwd=None):
        try:
            self.url = url
            self.usr = usr
            self.pwd = pwd

            if self.url and self.usr and self.pwd:
                self.kg_graph = Graph(self.url, username=self.usr, password=self.pwd)
                print("neo4j init ok !", self.url, self.usr, self.pwd)
            else:
                print("neo4j 参数缺失!")
                raise ValueError
        except Exception as e:
            print("neo4j error:", e, self.url, self.usr, self.pwd)
            self.kg_graph = None

    def get_label_list(self):
        """
        获取neo4j 所有的label [{'value': 'Movie'}, {'value': 'Person'}, ..]
        """
        cql = 'MATCH (n) RETURN DISTINCT labels(n)'
        result = self.kg_graph.run(cql)
        label_list = []
        for label_dict in result.data():
            for labels_item in label_dict['labels(n)']:
                if labels_item not in label_list:
                    tmp = {'value': labels_item}
                    label_list.append(tmp)
        return label_list

    def insert_node(self, name,type, prop_dict):

        cql = "CREATE({name}:{type} {prop})".format(name=name,type=type,prop=str(prop_dict))
        # cql = "MERGE (a:test {name: 'qqc'}) RETURN a"
        # label_list = ":" + ":".join(label_list)
        # print("label_list", label_list)
        # CREATE(BicentennialMan: Movie
        # cql = "MERGE (a{label} {prop}) RETURN a".format(label=label_list, prop=str(prop_dict))
        print(cql)
        # return self.kg_graph.run(cql)

    def delete(self, obj):
        self.kg_graph.delete(obj)

    def separate(self, obj):
        """ 用于删除节点之间的关系"""
        self.kg_graph.separate(obj)

    def find_relation_by_triple(self, start_node, labels_start_list, relationship, end_node_name, labels_end_list):
        """ 根据三元组 找到 指定关系对象"""
        start_node_obj = self.get_node(*labels_start_list, name=start_node)
        end_node_obj = self.get_node(*labels_end_list, name=end_node_name)
        relation_obj = self.kg_graph.match_one([start_node_obj], r_type=relationship)
        print("end1:", relation_obj, relation_obj is None, start_node_obj, end_node_obj)
        print("end2:", relation_obj.end_node, end_node_obj)
        if relation_obj is not None:
            if relation_obj.end_node == end_node_obj:
                # print("p匹配")
                return relation_obj
        return None

    def find_node(self, *label, **kwargs):
        """
        查找结点  find_node(label1, label2, name="台东县")
        :param node_label: args   (label1, label2)
        :param node_properties: kwargs  ( name="台东县")
        :return: nodes mather
        """
        return self.kg_graph.nodes.match(*label, **kwargs)

    def find_node_by_name(self, label, name):
        """
        查找结点  find_node(label1, label2, name="台东县")
        :param node_label: args   (label1, label2)
        :param node_properties: kwargs  ( name="台东县")
        :return: nodes mather
        """
        return self.kg_graph.nodes.match(label, name__contains=name)

    def find_node_new(self, node_label_list=None, node_properties_dict=None):
        """
        查找结点  参数: 列表 字典
        :return: nodes mather
        """
        return self.find_node(*node_label_list, **node_properties_dict)

    def get_node(self, *node_label, **node_properties):
        """    test_graph.get_node(name="台东县")  """
        result = self.find_node(*node_label, **node_properties)
        # print(list(result))
        if result:
            return result.first()
        return None

    def is_exist(self, *node_label, **node_properties):
        """ 判断当前节点是否存在 """
        if len(self.find_node(*node_label, **node_properties)) > 0:
            return True
        return False

    def add_node(self, *node_label, **node_properties):
        if self.is_exist(*node_label, **node_properties) is False:
            test_node = Node(*node_label, **node_properties)
            tmp = self.kg_graph.create(test_node)
            print("add new node:", node_label, node_properties)
            return tmp
        else:
            print("add node existed:", node_label, node_properties)

    def add_node_new(self, node_label, node_properties_dict):
        """
        添加节点
        :param node_label: <list> or <str>
        :param node_properties_dict: <dict>
        ['省市'] {'name': '台湾省'}

        """
        node_label_list = None
        if isinstance(node_label, str):
            node_label_list = [node_label]
        elif isinstance(node_label, list):
            node_label_list = node_label
        else:
            print("add_node_new: error type !  node_label must be list/ str ! now node_label:", type(node_label))

        return self.add_node(*node_label_list, **node_properties_dict)

    def add_relationship(self, start_node_name, relation_str, end_node_name, **kwargs):
        """ 添加关系"""
        start_node = self.find_node(start_node_name)
        end_node = self.find_node(end_node_name)
        if start_node is not None and end_node is not None:
            relation_obj = Relationship(start_node, relation_str, end_node, **kwargs)
            self.kg_graph.create(relation_obj)
            # print("add new relation:", start_node_name, relation_str, end_node_name, kwargs)
            return relation_obj
        else:
            # print("add relation failed:", start_node_name, relation_str, end_node_name, kwargs)
            return None

    def add_relationship_new(self, start_node, relation_str, end_node, **kwargs):
        """ 通过node对象添加 """
        if start_node is not None and end_node is not None:
            relation_obj = Relationship(start_node, relation_str, end_node, **kwargs)
            self.kg_graph.create(relation_obj)
            # print("add new relation:", relation_str)
            return relation_obj
        else:
            # print("add relation failed:", relation_str)
            return None

    def match_sub_info(self, node_name):
        """ 差选当前节点下的子节点和关系 """
        sub_nodes_list = []
        sub_relationship_list = []
        label_list = []

        node_obj = self.get_node(name=node_name)
        start_node_label = str(node_obj.labels).split(":")[1:][0]  # 默认不会搜索属性
        # print("node:", str(node_obj.labels).split(":")[1:])
        for rel in self.kg_graph.match(nodes=(node_obj,), ):
            info = {
                "start_node": rel.start_node["name"],
                'end_node': rel.end_node["name"],
                'relationship': rel._type,
            }
            end_node_labels = str(rel.end_node.labels).split(":")[1:]
            print("node:", rel.end_node["name"], end_node_labels)

            for label_item in end_node_labels:
                if label_item not in label_list:
                    label_list.append(label_item)

            sub_nodes_list.append(rel.end_node["name"])
            sub_relationship_list.append(info)

        # print(sub_nodes_list, sub_relationship_list, label_list)
        print(label_list)
        return node_name, start_node_label, label_list

    def match_virtual_sub_info(self, node_name, label_name):
        """ 差选当前节点下的子节点和关系 """
        sub_nodes_list = []

        node_obj = self.get_node(name=node_name)
        # start_node_label = str(node_obj.labels).split(":")[1:][0]  # 默认不会搜索属性
        # print("node:", str(node_obj.labels).split(":")[1:])
        for rel in self.kg_graph.match(nodes=(node_obj,), ):
            # info = {
            #     "start_node": rel.start_node["name"],
            #     'end_node': rel.end_node["name"],
            #     'relationship': rel._type,
            # }

            end_node_labels = str(rel.end_node.labels).split(":")[1:]
            if label_name in end_node_labels:
                print("node:", rel.end_node["name"], end_node_labels)
                sub_nodes_list.append(rel.end_node["name"])

        # print(node_name, label_name, sub_nodes_list)
        return node_name, label_name, sub_nodes_list

    def get_sub_info_by_name(self, node_name, class_name):
        """ get_sub_info_by_name("台东县", '气象') """
        sub_nodes_list = []
        sub_relationship_list = []
        node_ojb = self.get_node(name=node_name)
        for rel in self.kg_graph.match(nodes=(node_ojb,), ):
            info = {
                "start_node": rel.start_node["name"],
                'end_node': rel.end_node["name"],
                'relationship': rel._type,
            }
            print("node:", rel.end_node["name"], str(rel.end_node.labels).split(":")[1:])

            if class_name in str(rel.end_node.labels).split(":")[1:]:
                sub_nodes_list.append(rel.end_node["name"])
                sub_relationship_list.append(info)
        print(sub_nodes_list)
        print(sub_relationship_list)
        return sub_nodes_list, sub_relationship_list

    def get_sub_info(self, *node_label, **node_properties):
        """ 获取下层的信息"""
        nodes_list = []
        relationship_list = []

        node = self.get_node(*node_label, **node_properties)
        root_node_name, root_node_labels = node['name'], str(node.labels).split(":")[1:]
        # print({'name': root_node_name, "category": root_node_labels, })
        nodes_list.append({'name': root_node_name, "category": root_node_labels, })

        for rel in self.kg_graph.match(nodes=(node,)):
            node_tmp = {
                'name': rel.end_node["name"],
                "category": str(rel.end_node.labels).split(":")[1:],  # :Country => Country
            }
            # print(node_tmp)
            relationship_tmp = {
                "source": rel.start_node["name"],
                'target': rel.end_node["name"],
                'value': rel._type,
            }
            nodes_list.append(node_tmp)
            relationship_list.append(relationship_tmp)
        # print(nodes_list)
        # print(relationship_list)
        return nodes_list, relationship_list

    def get_sub_info2(self, *node_label, **node_properties):
        nodes_list = []
        relationship_list = []

        node = self.get_node(*node_label, **node_properties)
        root_node_name, root_node_label = node['name'], str(node.labels)[1:]
        nodes_list.append({'name': root_node_name, "category": root_node_label, })

        for rel in self.kg_graph.match(nodes=(node,)):
            node_tmp = {
                'name': rel.end_node["name"],
                "category": str(rel.end_node.labels)[1:],  # :Country => Country
            }
            relationship_tmp = {
                "source": rel.start_node["name"],
                'target': rel.end_node["name"],
                'value': rel._type,
            }
            nodes_list.append(node_tmp)
            relationship_list.append(relationship_tmp)
        # print(nodes_list)
        # print(relationship_list)
        return nodes_list, relationship_list

        # self.match_sub_info(node)

    def add_info_triple(self, start_node_name, labels_start_list, relationship, end_node_name, labels_end_list):
        """
        插入三元组信息
        多标签：('台湾', ['省市'], '中文名称', '台湾', ['省市','标签2'])
        单标签 ['台湾省', ['省'], 'label', '省', ['属性']]
        """
        # start node
        if labels_start_list:  # 查找,
            start_node_obj = self.find_node(*labels_start_list, name=start_node_name).first()
        else:
            start_node_obj = self.find_node(name=start_node_name).first()
        if not start_node_obj:  # 不存在则创建
            self.add_node_new(labels_start_list, {'name': start_node_name, })
            start_node_obj = self.find_node(*labels_start_list, name=start_node_name).first()

        if "属性" not in labels_end_list:  # 插入实体-关系-实体
            # end node
            if labels_end_list:
                end_node_obj = self.find_node(*labels_end_list, name=end_node_name).first()
            else:
                end_node_obj = self.find_node(name=end_node_name).first()
            if not end_node_obj:
                # print('add,', end_node_name)
                self.add_node_new(labels_end_list, {'name': end_node_name, })
                end_node_obj = self.find_node(*labels_end_list, name=end_node_name).first()

            # print("add rel : ", start_node_obj, relationship, end_node_obj)
            self.add_relationship_new(start_node_obj, relationship, end_node_obj)

        else:  # 插入 实习-属性-属性值
            self.set_property(start_node_obj, {relationship: end_node_name})

        print("{}:{}--{}--{}:{} ok!".format(start_node_name, labels_start_list, relationship, end_node_name,
                                            labels_end_list))

    def add_info_triple_list(self, triple_list):
        for triple_item in triple_list:
            # print(*triple_item)
            self.add_info_triple(*triple_item)

    # def delete_obj(self, obj):
    #     self.kg_graph.delete(obj)
    #     print("delete ok!")

    def delete_all(self):
        self.kg_graph.delete_all()
        print("delete all!")

    ################################
    # 节点.类别.关系查询
    def generate_graph_data(self, node_obj_list):
        """
        遍历 node_obj_list 的获取节点之间的关系 生成 node list 数据
         node {"id": str(node_id), 'name': node_name, 'labels': node_labels}
         link { "id": str(link_item.identity), "source": str(start_node.identity), "source_name": str(start_node['name']), 'value': link_item._type, 'target': str(end_node.identity), 'target_name': str(end_node['name']), }
        """
        node_list, link_list, cate = [], [], []
        link_obj_list = []  # 节点对象列表
        for node_item in node_obj_list:
            # node_id = node_item.identity
            # node_name = node_item['name']
            # node_labels = self.format_label(node_item.labels)
            # tmp = {"id": str(node_id), 'name': node_name, 'labels': node_labels}
            tmp = {"id": str(node_item.identity), 'name': node_item['name'], 'labels': list(node_item._labels)}
            node_list.append(tmp)

            for link_item in self.kg_graph.match(nodes=set([node_item])):
                if link_item not in link_obj_list:
                    link_obj_list.append(link_item)

                    start_node, end_node = link_item.nodes
                    if start_node in node_obj_list and end_node in node_obj_list:
                        link_tmp_dict = {
                            "id": str(link_item.identity),
                            "source": str(start_node.identity),
                            "source_name": str(start_node['name']),
                            'value': link_item._type,
                            'target': str(end_node.identity),
                            'target_name': str(end_node['name']),
                        }
                        link_list.append(link_tmp_dict)

        # print("node_list: ", node_list)
        # print("link_list: ", link_list)
        return node_list, link_list

    def generate_graph_data2(self, node_obj_list, rel_obj_list):
        node_list = []
        rel_list = []
        for node in node_obj_list:
            tmp = {"id": str(node.identity), 'name': node['name'], 'labels': list(node._labels)}
            node_list.append(tmp)

        for rel in rel_obj_list:
            tmp = {
                "id": str(rel.identity),
                "source": str(rel.start_node.identity),
                "source_name": str(rel.start_node['name']),
                'value': rel._type,
                'target': str(rel.end_node.identity),
                'target_name': str(rel.end_node['name']),
            }
            rel_list.append(tmp)
        return node_list, rel_list

    def get_category_graph(self, node_list, link_list):
        """生成对应的带有类别的图"""
        categories_list = []
        new_node_list = []
        for node_item in node_list:
            # 以后改成从指定的类别属性中拿
            node_labels = node_item.get('labels')
            if 'label' in node_item:  # 如果节点存在label属性，获取
                label = node_item.get('label')
            elif len(node_labels) == 1:  # 节点label个数为1
                label = node_labels[0]
            else:
                label = node_labels[0]  # 节点多label取第一个

            new_node_list.append({'id': node_item['id'], 'name': node_item['name'], 'category': label})
            tmp = {'name': label}
            if tmp not in categories_list:
                categories_list.append(tmp)
        return new_node_list, link_list, categories_list

    # 节点查询
    def node_query_fuzzy(self, input_str):
        """ 节点名称查询(支持模糊查询 name__contains) """
        # {4: {'id': 4, 'name': '测试节点3', 'labels': ['测试'], 'obj': (_3291:测试 {id: 4, name: '\u6d4b\u8bd5\u8282\u70b93'})}
        node_result = self.kg_graph.nodes.match(name__contains=input_str)  # 模糊查询
        node_obj_list = list(node_result)
        node_list, link_list = self.generate_graph_data(node_obj_list)  # 根据节点列表生成 关系和节点数据

        return node_list, link_list

    def node_query(self, input_str):
        """ 节点名称查询精确查询 """
        # {4: {'id': 4, 'name': '测试节点3', 'labels': ['测试'], 'obj': (_3291:测试 {id: 4, name: '\u6d4b\u8bd5\u8282\u70b93'})}
        node_result = self.kg_graph.nodes.match(name=input_str)
        node_obj_list = list(node_result)
        # print('node_obj_list', node_obj_list)
        if node_obj_list:
            node_list, link_list = self.expand_by_node_id(node_obj_list[0].identity, [], True)
            return node_list, link_list
        else:
            return [], []

    # 类别查询
    def label_query(self, input_str):
        """ 类别查询"""

        node_result = self.kg_graph.nodes.match(input_str)
        node_obj_list = list(node_result)
        node_list, link_list = self.generate_graph_data(node_obj_list)  # 根据节点列表生成 关系和节点数据
        return node_list, link_list

    # 关系查询
    def rel_query(self, input_str):
        """"关系查询"""
        rel_result = self.kg_graph.match(r_type=input_str)
        rel_list = list(rel_result)
        node_list = []
        for rel_item in rel_result:
            start_node = rel_item.start_node
            end_node = rel_item.end_node
            if start_node not in node_list:
                node_list.append(start_node)
            if end_node not in node_list:
                node_list.append(end_node)
        print(node_list)
        print(rel_list)
        return self.generate_graph_data2(node_list, rel_list)

        # print(list(node_result)[0])
        # print(dir(list(node_result)[0]))
        # print(list(node_result)[0].type)
        # print(list(node_result)[0].types())
        # print(list(list(node_result)[0].types()))
        #
        # print("===")
        # print(dir(list(node_result)[0].start_node))
        # print(list(node_result)[0].start_node)
        # print(list(node_result)[0].start_node.types())
        # print(list(node_result)[0].start_node.labels)
        # print(list(node_result)[0].start_node._labels)
        # print(list(node_result)[0].start_node.has_label)
        # print(type(list(node_result)[0].start_node.labels))
        # t = list(node_result)[0].start_node.labels
        # print(t.__selected)

    def get_data_by_id_list(self, id_list):
        """ 根据 id list 生成graph 数据"""
        node_list = []
        for node_id in id_list:
            node_list.append(self.kg_graph.nodes.get(node_id))  # node id => node obj
        return self.generate_graph_data(node_list)

    def expand_by_node_id(self, node_id, node_id_list=None, contain_root=False):
        def get_sub_link(sub_node_obj, node_id_list=None):
            link_list = []
            if node_id_list:
                sub_rel_result = self.kg_graph.match(nodes=(set([sub_node_obj])))
                for sub_rel_obj in sub_rel_result:
                    start_node, end_node = sub_rel_obj.nodes
                    if start_node.identity in node_id_list and end_node.identity in node_id_list:
                        # print(sub_node_obj['name'], sub_rel_obj)
                        tmp = {
                            "id": str(sub_rel_obj.identity),
                            "source": str(start_node.identity),
                            "source_name": str(start_node['name']),
                            'value': sub_rel_obj._type,
                            'target': str(end_node.identity),
                            'target_name': str(end_node['name']),
                        }
                        # print("--sub:", tmp)
                        link_list.append(tmp)
            return link_list

        expand_node_list = []
        expand_link_list = []

        node_obj = self.kg_graph.nodes.get(node_id)
        if contain_root:
            expand_node_list.append(
                {"id": str(node_obj.identity), 'name': node_obj['name'], 'labels': self.format_label(node_obj.labels)}
            )

        rel_result = self.kg_graph.match(nodes=(set([node_obj])))
        # print(type(rel_result))
        sub_link_list = []
        for rel_obj in rel_result:
            start_node, end_node = rel_obj.nodes
            # print(start_node.labels, )
            if start_node != node_obj:  # 入度节点
                tmp = {"id": str(start_node.identity), 'name': start_node['name'],
                       'labels': self.format_label(start_node.labels)}
                # print("in: ", tmp)
                expand_node_list.append(tmp)
                if node_id_list:
                    node_id_list.append(start_node.identity)
                sub_link_list.extend(get_sub_link(start_node, node_id_list))

            if end_node != node_obj:  # 出度节点
                tmp = {"id": str(end_node.identity), 'name': end_node['name'],
                       'labels': self.format_label(end_node.labels)}
                # print("out: ", tmp)
                expand_node_list.append(tmp)
                if node_id_list:
                    node_id_list.append(end_node.identity)
                # sub_link_list = get_sub_link(end_node, node_id_list)
                sub_link_list.extend(get_sub_link(end_node, node_id_list))

            link_tmp_dict = {
                "id": str(rel_obj.identity),
                "source": str(start_node.identity),
                "source_name": str(start_node['name']),
                'value': rel_obj._type,
                'target': str(end_node.identity),
                'target_name': str(end_node['name']),
            }
            # print("link: ", link_tmp_dict)
            expand_link_list.append(link_tmp_dict)

        for sub_link in sub_link_list:
            if sub_link not in expand_link_list:
                expand_link_list.append(sub_link)

            # print(list(result))

        # print("sub_link:", sub_link_list)
        # print("expand_link_list: ", expand_link_list)
        return expand_node_list, expand_link_list

    def get_node_by_id(self, node_id):
        """通过id 获取node对象"""
        node_obj = self.kg_graph.nodes.get(node_id)
        return node_obj

    def get_link_by_id(self, link_id):
        link_obj = self.kg_graph.relationships.get(link_id)
        return link_obj

    def get_node_info_by_id(self, node_id):
        """通过id 获取node对象"""
        node_obj = self.get_node_by_id(node_id)
        prop_list = []
        for k, v in node_obj.items():
            if k != 'name':
                tmp = {'prop_name': k, "prop_value": v}
                prop_list.append(tmp)
        result_dict = {
            'node_name': node_obj['name'],
            'node_label': list(node_obj._labels),
            'props': prop_list

        }
        return result_dict

    def get_rel_by_id(self, rel_id):
        """通过id 获取rel对象"""
        rel_obj = self.kg_graph.relationships.get(rel_id)
        return rel_obj

    def set_property(self, obj, prop_dict):
        """ 设置对象属性 append 追加方式"""
        for k, v in prop_dict.items():
            obj[k] = str(v)
        self.kg_graph.push(obj)

    def update_prop(self, node_id, prop_dict):
        """
        根据prop_dict对node进行 添加\更新\删除操作
        :param node_id: <int>
        :param prop_dict: <dict>
        :return:
        """
        print("prop pre obj:", node_id, prop_dict)
        node_obj = self.get_node_by_id(node_id)
        del_props = node_obj.keys() - prop_dict.keys()  # 待删除的属性名称
        for prop_key in del_props:
            del node_obj[prop_key]
        node_obj.update(prop_dict)  # 添加和更新属性
        print("prop now obj:", node_obj)
        self.kg_graph.push(node_obj)

    def get_node_property(self, node_id):
        node_obj = self.kg_graph.nodes.get(node_id)
        return dict(node_obj)

    def update_node_property(self, node_id, prop_dict):
        node_obj = self.kg_graph.nodes.get(node_id)
        self.set_property(node_obj, prop_dict)
        # for k, v in prop_dict:
        #     node_obj[k] = v
        print("ok:")
        return True, None

    def crawl_update_props(self, item_dict):
        """
        爬虫更新属性信息
        :param item_dict:
        :return:
        """
        # item_dict = {'name': '台北市', 'label': '直辖市', 'lat': 25.03, 'lng': 121.5}
        item_dict = item_dict.copy()  # 下面有del
        node_obj = self.kg_graph.nodes.match(item_dict['label'], name=item_dict['name']).first()  # 模糊查询
        del item_dict['label']
        self.set_property(node_obj, item_dict)
        print('lat lon ok!: ', item_dict)

    def get_neo4j_labels(self):
        label_list = []
        neo4j_result = list(self.kg_graph.run('MATCH (n) RETURN DISTINCT labels(n)'))
        for i in neo4j_result:
            print()
            if i['labels(n)'] not in label_list:
                label_list += i['labels(n)']
            label_list = list(set(label_list))
        return label_list

    def get_neo4j_static(self):
        """获取 节点数 关系数 类别数"""
        result = dict()
        result['node_num'] = list(self.kg_graph.run('MATCH (n) RETURN count(n)'))[0].values()[0]
        result['rel_num'] = list(self.kg_graph.run('MATCH p=()-->() RETURN count(p)'))[0].values()[0]
        result['label_num'] = len(list(self.kg_graph.run('MATCH (n) RETURN DISTINCT labels(n)')))
        return result

    def get_node_id_by_name(self, node_name):
        node_result = self.kg_graph.nodes.match(name=node_name).first()
        # print('get_node_id_by_name:', node_result.identity, node_result, )
        if node_result is None:
            return None
        return node_result.identity

    def get_node_by_name(self, node_name):
        node_result = self.kg_graph.nodes.match(name=node_name).first()
        return node_result

    def get_node_label_by_name(self, node_name):
        """ 返回 label list"""
        node_result = self.kg_graph.nodes.match(name=node_name).first()  # 模糊查询
        # print('get_node_id_by_name:', node_result.identity, node_result, )
        return list(node_result._labels)

    def get_node_geo_by_id(self, node_id):
        node_obj = self.get_node_by_id(node_id)
        geo_dict = {}
        # print(list(node_obj._labels))
        node_label = list(node_obj._labels)[0]
        if 'lat' in node_obj and 'lon' in node_obj:
            geo_dict = {
                'markers': {
                    node_label: [
                        {'name': node_obj['name'], 'lat': node_obj['lat'], 'lon': node_obj['lon']},
                    ]
                }
            }
        # print(geo_dict)
        return geo_dict

    def format_label(self, raw_label_str):
        """ """
        raw_label_str = str(raw_label_str)
        return raw_label_str.split(':')[1:]


if __name__ == '__main__':
    client = KgNeo4j(url='bolt://localhost:11002', usr="Avation Safety", pwd="123")
    prop_dict = {'Status': 'Preliminary', 'Date': 'Saturday 2 August 1919', 'Type': 'Caproni Ca.48', 'Operator': 'Caproni', 'Registration': ' registration unknown', 'C/n / msn': ' ', 'First flight': ' 1919', 'Engines': ' 3 Liberty L-12', 'Crew': 'Fatalities: 2 / Occupants: 2', 'Passengers': 'Fatalities: 12 / Occupants: 12', 'Total': 'Fatalities: 14 / Occupants: 14 ', 'Aircraft damage': ' Destroyed', 'Aircraft fate': ' Written off (damaged beyond repair)', 'Location': 'Verona ( \xa0 Italy) \r\n', 'Phase': ' En route (ENR)', 'Nature': 'Passenger', 'Departure airport': 'Venice-Marco Polo Airport (VCE/LIPZ), Italy', 'Destination airport': 'Milano-Taliedo Airport, Italy', 'Narrative': "The Caproni Ca.48 took off from the company's home airfield Milano-Taliedo Airport, Italy, at 07:30 local time for a flight to Venice, where it arrived without incident at 09:22. The aircraft took off at 17:00 for the return flight to Taliedo. Eyewitnesses reported that as the airliner passed near the airfield at Verona at an altitude of 3,000 feet (910 m), its wings seemed first to flutter and then to collapse entirely. Several of the people on board jumped from the aircraft to their deaths before it crashed. There were no survivors.Different sources put the death toll at 14, 15 or 17.The Ca.48, a large triplane, was an airliner conversion of the Caproni Ca.42 heavy bomber."}
    # print(prop_dict['Date'])
    name = prop_dict['Date']+"_"+prop_dict['Type']+"_"+prop_dict['Departure airport']+"_"+prop_dict['Destination airport']
    type = "accident"
    name = name.replace(" ",'_')
    client.insert_node(name,type,prop_dict)
    # print(client.node_query('台东县'))
    # client.update_prop(1253, {"name": "火灾", '测试': 1})
    # # 显示当前数据库的所有Label信息
    # print("label:", client.get_label_list())

    # 添加三元组
    # client.add_info_triple(*['t3', ['t'], 'to', 't2', ['t']])
    # client.add_info_triple(*['t3', ['t'], 'label', 't_1', ['属性']])

    # node_list, link_list = client.node_query("花莲县")
    # node_list, link_list, categories_list = client.get_category_graph(node_list, link_list)
    # print(node_list)
    # print(link_list)
    # print(categories_list)

    # client.get_node_id_by_name('台中港')
    # t = client.get_node_info_by_id(7307)
    # t = client.get_neo4j_labels()
    # a = {'name': '台北市', 'label': '直辖市', 'lat': 25.03, 'lng': 121.5}
    # t = client.get_link_by_id(10920)
    # t = client.find_relation_by_triple('测试节点1', ['测试'], 'to2', '测试节点2', ['测试'])
    # print("t:", t, type(t))
    # client.separate(t)

    # obj = client.get_node_by_id(7307)
    # client.set_property(obj, {'name': '测试节点', 'test1': 1, 'test2': 2})

    # t = client.node_query('台东县')
    # print(t[0])
    # print(t[1])
    # t = client.get_node_by_id(10779)
    # for k, v in t.items():
    #     print(k, v)

    # print(dir(client.get_node_by_id(7307)))
    # print(t.labels)
    # print(client.add_info_triple('测试节点1', ['测试'], '水文气象', 'xx的潮汐', ['水文气象']))
    # t = client.get_node_by_id(10774)
    # print(t.get('name'))
    # print(client.get_neo4j_static())
    # print(list(client.find_node(name='测试105')))
    # print(client.add_node('测试', name='测试150'))
    #
    # t = client.kg_graph.run('MATCH (n) RETURN DISTINCT labels(n)')
    # a = list(t)[1].values()
    # print(a)
    # print(len(list(t)))
    # print(client.node_query("台东县"))
    # print(client.rel_query("to"))
    # print(client.get_data_by_id_list(
    #     [9189, 9188, 9187, 9186, 9185, 9184, 9183, 9182, 9181, 9180, 9179, 9178, 9177, 9176, 9175, 9174, 9173, 9172,
    #      9171, 9170, 9169, 9168]))
