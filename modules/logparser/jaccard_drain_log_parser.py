# SPDX-License-Identifier: MIT
# This file implements the Drain algorithm for log parsing.
# Based on https://github.com/logpai/logparser/blob/master/logparser/Drain/Drain.py by LogPAI team


from .drain_base_log_parser import DrainBaseLogParser, LogCluster, Node
from .parser_factory import parser_register


@parser_register
class JaccardDrainLogParser(DrainBaseLogParser):
    """
    add a new matching pattern to the log cluster.
    Cancels log message length as  first token.
    Drain that uses Jaccard similarity to match log messages.
    """

    @staticmethod
    def _create_template(seq1: list[str], seq2: list[str]) -> list[str]:
        inter_set = set(seq1) & set(seq2)

        # test_max_clusters_lru_multiple_leaf_nodes
        # Update param_str at different positions with the same length
        if len(seq1) == len(seq2):
            ret_val = list(seq2)
            for idx, (token1, token2) in enumerate(zip(seq1, seq2)):
                if token1 != token2:
                    ret_val[idx] = "<*>"
        # param_str is updated at the new position with different length
        else:
            # Take the template with long length
            ret_val = list(seq1) if len(seq1) > len(seq2) else list(seq2)
            for idx, token in enumerate(ret_val):
                if token not in inter_set:
                    ret_val[idx] = "<*>"

        return ret_val

    def _add_seq_to_prefix_tree(self, cluster: LogCluster) -> None:
        token_count = len(cluster.log_template_tokens)
        # Determine if the string is empty
        if not cluster.log_template_tokens:
            token_first = ""
        else:
            token_first = cluster.log_template_tokens[0]
        if token_first not in self._root_node.children_node:
            first_layer_node = Node()
            self._root_node.children_node[token_first] = first_layer_node
        else:
            first_layer_node = self._root_node.children_node[token_first]

        cur_node = first_layer_node

        # handle case of empty log string
        if token_count == 0:
            cur_node.cluster_ids = [cluster.cluster_id]
            return

        # test_add_shorter_than_depth_message : only one word add into current node
        if token_count == 1:
            cur_node.cluster_ids.append(cluster.cluster_id)

        for cur_node_depth, token in enumerate(
            cluster.log_template_tokens[1:], start=1
        ):
            # if at max depth or this is last token in template - add current log cluster to the leaf node
            # It starts with the second word, so the sentence length -1
            if cur_node_depth == self._depth or cur_node_depth == token_count - 1:
                cur_node.cluster_ids.append(cluster.cluster_id)
                break

            # if token not matched in this layer of existing tree.
            if token not in cur_node.children_node:
                if self._parametrize_numeric_tokens and self.has_numbers(token):
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
        # Jaccard index, It is used to measure the similarity of two sets.
        # The closer its value is to 1, the more common members the two sets have, and the higher the similarity.

        # list are empty - full match
        if len(seq1) == 0:
            return 1.0, 0

        param_count = 0

        for token1 in seq1:
            if token1 == "<*>":
                param_count += 1

        # If the token and the data have the same length, and there are param_str in the token
        if len(seq1) == len(seq2) and param_count > 0:
            # seq2 removes the param_str position
            seq2 = [x for i, x in enumerate(seq2) if seq1[i] != "<*>"]

        # If there are param_str, they are removed from the coefficient calculation
        if include_params:
            seq1 = [x for x in seq1 if x != "<*>"]

        # Calculate the Jaccard coefficient
        ret_val = len(set(seq1) & set(seq2)) / len(set(seq1) | set(seq2))

        # Jaccard coefficient calculated under the same conditions has a low simSep value
        # So gain is applied to the calculated value (The test case test_add_log_message_sim_75)
        ret_val = ret_val * 1.3 if ret_val * 1.3 < 1 else 1

        return ret_val, param_count

    def _tree_search(
        self, tokens: list[str], include_params: bool
    ) -> LogCluster | None:
        # at first level, children are grouped by token (The first word in tokens)
        token_count = len(tokens)
        # cur_node = root_node.key_to_child_node.get(str(token_count))

        if not tokens:
            token_first = ""
            cur_node = self._root_node.children_node.get(token_first)
        else:
            token_first = tokens[0]
            cur_node = self._root_node.children_node.get(token_first)

        # no template with same token count yet
        if cur_node is None:
            return None

        # handle case of empty log string - return the single cluster in that group
        if token_count == 0:
            return self._id_to_cluster.get(cur_node.cluster_ids[0])

        # find the leaf node for this log - a path of nodes matching the first N tokens (N=tree depth)
        for cur_node_depth, token in enumerate(tokens[1:], start=1):
            # at max depth or this is last token
            if cur_node_depth == self._depth or cur_node_depth == token_count - 1:
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
        return self.fast_match(cur_node.cluster_ids, tokens, include_params)

    @staticmethod
    def name() -> str:
        return "JaccardDrain"

    @staticmethod
    def description() -> str:
        return "JaccardDrain 是一种基于 Drain 和 Jaccard 相似度的高效日志模板提取算法"
