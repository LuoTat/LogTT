# SPDX-License-Identifier: MIT
# This file implements the Drain algorithm for log parsing.
# Based on https://github.com/logpai/logparser/blob/master/logparser/Drain/Drain.py by LogPAI team


from .drain_base_log_parser import Content, DrainBaseLogParser, LogCluster, Node, Token
from .parser_factory import parser_register


@parser_register
class JaccardDrainLogParser(DrainBaseLogParser):
    @staticmethod
    def _create_template(content1: Content, content2: Content) -> Content:
        # Update param_str at different positions with the same length
        if len(content1) == len(content2):
            return [
                token1 if token1 == token2 else "<*>"
                for token1, token2 in zip(content1, content2)
            ]

        # param_str is updated at the new position with different length
        # Take the template with long length
        inter_set = set(content1) & set(content2)
        ret_val = content1 if len(content1) > len(content2) else content2
        for idx, token in enumerate(ret_val):
            if token not in inter_set:
                ret_val[idx] = "<*>"
        return ret_val

    def _add_seq_to_prefix_tree(self, cluster: LogCluster) -> None:
        content_length = len(cluster.content)
        cur_node = self._root_node
        for cur_node_depth, token in enumerate(cluster.content, start=1):
            # if at max depth or this is last token in template - add current log cluster to the leaf node
            if cur_node_depth == self._depth or cur_node_depth == content_length + 1:
                cur_node.cluster_ids.append(cluster.cluster_id)
                return

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
        content1: Content,
        content2: Content,
        include_params: bool,
    ) -> tuple[float, int]:
        # content1 is a template, content2 is the log to match
        # Jaccard index, It is used to measure the similarity of two sets.
        # The closer its value is to 1, the more common members the two sets have, and the higher the similarity.

        # list are empty - full match
        if len(content1) == 0:
            return 1.0, 0

        param_count = 0

        for token1 in content1:
            if token1 == "<*>":
                param_count += 1

        # If the token and the data have the same length, and there are param_str in the token
        if len(content1) == len(content2) and param_count > 0:
            # seq2 removes the param_str position
            content2 = [
                token for idx, token in enumerate(content2) if content1[idx] != "<*>"
            ]

        # If there are param_str, they are removed from the coefficient calculation
        if include_params:
            # 参数位不参与惩罚
            content1 = [token for token in content1 if token != "<*>"]

        # Calculate the Jaccard coefficient
        sim = len(set(content1) & set(content2)) / len(set(content1) | set(content2))

        # Jaccard coefficient calculated under the same conditions has a low simSep value
        # So gain is applied to the calculated value
        sim = min(sim * 1.3, 1.0)

        return sim, param_count

    def _tree_search(
        self,
        content: Content,
        include_params: bool,
    ) -> LogCluster | None:
        content_length = len(content)
        cur_node = self._root_node
        for cur_node_depth, token in enumerate(content, start=1):
            # at max depth or this is last token
            if cur_node_depth == self._depth or cur_node_depth == content_length + 1:
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
        return "JaccardDrain"

    @staticmethod
    def description() -> str:
        return "JaccardDrain 是一种基于 Drain 和 Jaccard 相似度的高效日志模板提取算法。"
