# SPDX-License-Identifier: MIT
# This file implements the Drain algorithm for log parsing.
# Based on https://github.com/logpai/logparser/blob/master/logparser/Drain/Drain.py by LogPAI team


from itertools import chain

from .base_log_parser import Token
from .drain_base_log_parser import Content, DrainBaseLogParser, LogCluster, Node
from .parser_factory import parser_register


@parser_register
class DrainLogParser(DrainBaseLogParser):
    @staticmethod
    def _create_template(content1: Content, content2: Content) -> Content:
        assert len(content1) == len(content2)
        return [
            token1 if token1 == token2 else "<*>"
            for token1, token2 in zip(content1, content2)
        ]

    def _add_seq_to_prefix_tree(self, cluster: LogCluster) -> None:
        content_length = len(cluster.content)
        cur_node = self._root_node
        for cur_node_depth, token in enumerate(
            chain([Token(content_length)], cluster.content),
            start=1,
        ):
            # if at max depth or this is last token in template - add current log cluster to the leaf node
            if cur_node_depth == self._depth or cur_node_depth == content_length + 2:
                cur_node.cluster_ids.append(cluster.cluster_id)
                return

            # if token not matched in this layer of existing tree.
            if token not in cur_node.children_node:
                if len(cur_node.children_node) + 1 < self._children:
                    # 如果当前节点不是最后一个节点，就添加一个新的节点
                    new_node = Node()
                    cur_node.children_node[token] = new_node
                    cur_node = new_node
                elif len(cur_node.children_node) + 1 == self._children:
                    # 如果当前节点是最后一个节点，就添加一个新的通配符节点
                    # 注意由于 _parametrize_numeric_tokens 的存在，可能已经存在一个通配符节点了，所以需要先检查一下
                    new_node = Node()
                    cur_node.children_node["<*>"] = new_node
                    cur_node = new_node
                else:
                    # 如果当前节点已满，就直接使用通配符节点
                    cur_node = cur_node.children_node["<*>"]

            # if the token is matched
            else:
                cur_node = cur_node.children_node[token]

    @staticmethod
    def _get_seq_distance(
        content1: Content,
        content2: Content,
        include_params: bool,
    ) -> tuple[float, int]:
        # content1 is a template, content2 is the log to match
        assert len(content1) == len(content2)

        # list are empty - full match
        if len(content1) == 0:
            return 1.0, 0

        sim_tokens = 0
        param_count = 0

        for token1, token2 in zip(content1, content2):
            if token1 == "<*>":
                param_count += 1
                continue
            if token1 == token2:
                sim_tokens += 1

        if include_params:
            # 参数位也当匹配贡献
            sim_tokens += param_count

        return float(sim_tokens) / len(content1), param_count

    def _tree_search(self, content: Content, include_params: bool) -> LogCluster | None:
        content_length = len(content)
        cur_node = self._root_node

        for cur_node_depth, token in enumerate(
            chain([Token(content_length)], content),
            start=1,
        ):
            # at max depth or this is last token
            if cur_node_depth == self._depth or cur_node_depth == content_length + 2:
                break

            if token in cur_node.children_node:
                cur_node = cur_node.children_node[token]
            elif (
                "<*>" in cur_node.children_node
            ):  # no exact next token exist, try wildcard node
                cur_node = cur_node.children_node["<*>"]
            else:  # no wildcard node exist
                return None

        # get best match among all clusters with same prefix, or None if no match is above sim_th
        return self._fast_match(cur_node.cluster_ids, content, include_params)

    @staticmethod
    def name() -> str:
        return "Drain"

    @staticmethod
    def description() -> str:
        return "Drain 是一种基于树结构的高效日志模板提取算法。"
