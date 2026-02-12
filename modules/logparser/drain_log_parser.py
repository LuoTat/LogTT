# SPDX-License-Identifier: MIT
# This file implements the Drain algorithm for log parsing.
# Based on https://github.com/logpai/logparser/blob/master/logparser/Drain/Drain.py by LogPAI team


from .drain_base_log_parser import DrainBaseLogParser, LogCluster, Node
from .parser_factory import parser_register


@parser_register
class DrainLogParser(DrainBaseLogParser):
    """Drain算法的实现类"""

    @staticmethod
    def _create_template(seq1: list[str], seq2: list[str]) -> list[str]:
        assert len(seq1) == len(seq2)
        return [
            token1 if token1 == token2 else "<*>" for token1, token2 in zip(seq1, seq2)
        ]

    def _add_seq_to_prefix_tree(self, cluster: LogCluster) -> None:
        token_count = len(cluster.log_template_tokens)
        token_count_str = str(token_count)
        if token_count_str not in self._root_node.children_node:
            first_layer_node = Node()
            self._root_node.children_node[token_count_str] = first_layer_node
        else:
            first_layer_node = self._root_node.children_node[token_count_str]

        cur_node = first_layer_node

        # handle case of empty log string
        if token_count == 0:
            cur_node.cluster_ids = [cluster.cluster_id]
            return

        for cur_node_depth, token in enumerate(cluster.log_template_tokens, start=1):
            # if at max depth or this is last token in template - add current log cluster to the leaf node
            if cur_node_depth == self._depth or cur_node_depth == token_count:
                cur_node.cluster_ids.append(cluster.cluster_id)
                break

            # if token not matched in this layer of existing tree.
            if token not in cur_node.children_node:
                if self._parametrize_numeric_tokens and DrainBaseLogParser._has_numbers(
                    token
                ):
                    if "<*>" not in cur_node.children_node:
                        new_node = Node()
                        cur_node.children_node["<*>"] = new_node
                        cur_node = new_node
                    else:
                        cur_node = cur_node.children_node["<*>"]

                else:
                    if len(cur_node.children_node) + 1 < self._max_children:
                        # 如果当前节点不是最后一个节点，就添加一个新的节点
                        new_node = Node()
                        cur_node.children_node[token] = new_node
                        cur_node = new_node
                    elif len(cur_node.children_node) + 1 == self._max_children:
                        # 如果当前节点是最后一个节点，就添加一个新的通配符节点
                        # 注意由于 _parametrize_numeric_tokens 的存在，可能已经存在一个通配符节点了，所以需要先检查一下
                        if "<*>" not in cur_node.children_node:
                            new_node = Node()
                            cur_node.children_node["<*>"] = new_node
                            cur_node = new_node
                        else:
                            cur_node = cur_node.children_node["<*>"]
                    else:
                        # 如果当前节点已满，就直接使用通配符节点
                        cur_node = cur_node.children_node["<*>"]

            # if the token is matched
            else:
                cur_node = cur_node.children_node[token]

    @staticmethod
    def _get_seq_distance(
        seq1: list[str], seq2: list[str], include_params: bool
    ) -> tuple[float, int]:
        # seq1 is a template, seq2 is the log to match
        assert len(seq1) == len(seq2)

        # list are empty - full match
        if len(seq1) == 0:
            return 1.0, 0

        sim_tokens = 0
        param_count = 0

        for token1, token2 in zip(seq1, seq2):
            if token1 == "<*>":
                param_count += 1
                continue
            if token1 == token2:
                sim_tokens += 1

        if include_params:
            sim_tokens += param_count

        return float(sim_tokens) / len(seq1), param_count

    def _tree_search(
        self, tokens: list[str], include_params: bool
    ) -> LogCluster | None:
        # at first level, children are grouped by token (word) count
        token_count = len(tokens)
        cur_node = self._root_node.children_node.get(str(token_count))

        # no template with same token count yet
        if cur_node is None:
            return None

        # handle case of empty log string - return the single cluster in that group
        if token_count == 0:
            return self._id_to_cluster[cur_node.cluster_ids[0]]

        # find the leaf node for this log - a path of nodes matching the first N tokens (N=tree depth)
        for cur_node_depth, token in enumerate(tokens, start=1):
            # at max depth or this is last token
            if cur_node_depth == self._depth or cur_node_depth == token_count:
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
        return self._fast_match(cur_node.cluster_ids, tokens, include_params)

    @staticmethod
    def name() -> str:
        return "Drain"

    @staticmethod
    def description() -> str:
        return "Drain 是一种基于树结构的高效日志模板提取算法"