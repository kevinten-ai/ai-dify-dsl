#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dify Workflow Manager v2.0
===========================

功能: 完整的Dify工作流管理工具，支持拆分、修改、重建、对比的完整工作流
作者: AI Assistant
日期: 2024-01-15

主要功能:
1. split  - 拆分YAML为独立文件（保留原始格式）
2. rebuild - 重建YAML文件
3. compare - 对比YAML差异
4. validate - 验证一致性

使用方法:
    python dify_workflow_manager_v2.py split "AI Code Review-V4.1.yml"
    python dify_workflow_manager_v2.py rebuild "parsed_workflow_V4.1_20240115_143022"
    python dify_workflow_manager_v2.py compare "original.yml" "rebuilt.yml"
"""

import os
import sys
import json
import re
import difflib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import logging
import jsonschema
from jsonschema import ValidationError
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 尝试导入ruamel.yaml（保留格式）,如果没有则回退到pyyaml
try:
    from ruamel.yaml import YAML
    YAML_AVAILABLE = True
except ImportError:
    import yaml
    YAML_AVAILABLE = False
    print("警告: ruamel.yaml未安装，将使用pyyaml（可能丢失格式信息）")
    print("建议安装: pip install ruamel.yaml")


class ErrorHandler:
    """错误处理器 - 提供完善的异常处理和恢复机制"""

    def __init__(self, max_retries: int = 3, enable_recovery: bool = True):
        self.max_retries = max_retries
        self.enable_recovery = enable_recovery
        self.error_log = []
        self.logger = logging.getLogger(__name__)

    def handle_operation(self, operation_func, operation_name: str, *args, **kwargs):
        """处理操作，包含重试和错误恢复"""
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                result = operation_func(*args, **kwargs)
                if attempt > 0:
                    self.logger.info(f"{operation_name} 在第 {attempt + 1} 次尝试后成功")
                return result
            except Exception as e:
                last_exception = e
                self.error_log.append({
                    'operation': operation_name,
                    'attempt': attempt + 1,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                })

                if attempt < self.max_retries - 1:
                    self.logger.warning(f"{operation_name} 第 {attempt + 1} 次失败，将重试: {e}")
                    # 可以在这里添加指数退避等策略
                else:
                    self.logger.error(f"{operation_name} 在 {self.max_retries} 次尝试后仍然失败: {e}")

        # 如果启用了恢复机制，尝试恢复操作
        if self.enable_recovery:
            return self._attempt_recovery(operation_name, last_exception, *args, **kwargs)

        raise last_exception

    def _attempt_recovery(self, operation_name: str, exception: Exception, *args, **kwargs):
        """尝试恢复失败的操作"""
        self.logger.info(f"尝试恢复操作: {operation_name}")

        # 根据操作类型尝试不同的恢复策略
        if 'split' in operation_name.lower():
            return self._recover_split_operation(operation_name, exception, *args, **kwargs)
        elif 'rebuild' in operation_name.lower():
            return self._recover_rebuild_operation(operation_name, exception, *args, **kwargs)
        elif 'validate' in operation_name.lower():
            return self._recover_validation_operation(operation_name, exception, *args, **kwargs)

        # 默认情况下重新抛出异常
        raise exception

    def _recover_split_operation(self, operation_name: str, exception: Exception, *args, **kwargs):
        """恢复拆分操作"""
        try:
            # 尝试创建基本的输出结构，即使某些节点失败
            if len(args) > 0 and hasattr(args[0], '_create_output_structure'):
                splitter = args[0]
                splitter._create_output_structure()
                self.logger.info("已创建基本的输出结构用于恢复")
                return True
        except Exception as recovery_error:
            self.logger.error(f"拆分操作恢复失败: {recovery_error}")

        raise exception

    def _recover_rebuild_operation(self, operation_name: str, exception: Exception, *args, **kwargs):
        """恢复重建操作"""
        try:
            # 尝试重建部分成功的节点
            if len(args) > 0 and hasattr(args[0], '_rebuild_yaml_structure'):
                rebuilder = args[0]
                # 尝试只重建成功的部分
                partial_structure = rebuilder._rebuild_yaml_structure(rebuilder._load_metadata())
                if partial_structure:
                    self.logger.info("已重建部分工作流结构")
                    return partial_structure
        except Exception as recovery_error:
            self.logger.error(f"重建操作恢复失败: {recovery_error}")

        raise exception

    def _recover_validation_operation(self, operation_name: str, exception: Exception, *args, **kwargs):
        """恢复验证操作"""
        # 对于验证操作，通常不需要恢复，直接返回验证失败的结果
        return False, [str(exception)]

    def generate_error_report(self, output_path: str = None) -> str:
        """生成错误报告"""
        if not self.error_log:
            return "无错误记录"

        report_lines = [
            "# 工作流处理错误报告",
            f"生成时间: {datetime.now().isoformat()}",
            f"错误数量: {len(self.error_log)}",
            "",
            "## 错误详情",
            ""
        ]

        for i, error in enumerate(self.error_log, 1):
            report_lines.extend([
                f"### 错误 {i}",
                f"- 操作: {error['operation']}",
                f"- 尝试次数: {error['attempt']}",
                f"- 时间: {error['timestamp']}",
                f"- 错误信息: {error['error']}",
                ""
            ])

        report_content = "\n".join(report_lines)

        if output_path:
            try:
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(report_content)
                self.logger.info(f"错误报告已保存到: {output_path}")
            except Exception as e:
                self.logger.error(f"保存错误报告失败: {e}")

        return report_content

    def clear_errors(self):
        """清除错误日志"""
        self.error_log.clear()


class DSLValidator:
    """Dify DSL结构验证器"""

    # Dify DSL JSON Schema定义
    DIFY_SCHEMA = {
        "type": "object",
        "required": ["app", "kind", "version", "workflow"],
        "properties": {
            "app": {
                "type": "object",
                "required": ["name", "mode"],
                "properties": {
                    "name": {"type": "string"},
                    "description": {"type": "string"},
                    "icon": {"type": "string"},
                    "icon_background": {"type": "string"},
                    "mode": {"type": "string", "enum": ["workflow", "chat", "completion"]}
                }
            },
            "kind": {"type": "string", "enum": ["app"]},
            "version": {"type": "string"},
            "dependencies": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "current_identifier": {"type": ["string", "null"]},
                        "type": {"type": "string", "enum": ["marketplace", "package"]},
                        "value": {"type": "object"}
                    }
                }
            },
            "workflow": {
                "type": "object",
                "required": ["graph"],
                "properties": {
                    "conversation_variables": {"type": "array"},
                    "environment_variables": {"type": "array"},
                    "features": {"type": "object"},
                    "graph": {
                        "type": "object",
                        "required": ["nodes", "edges"],
                        "properties": {
                            "nodes": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "required": ["id", "data"],
                                    "properties": {
                                        "id": {"type": "string"},
                                        "data": {
                                            "type": "object",
                                            "required": ["type", "title"],
                                            "properties": {
                                                "type": {"type": "string"},
                                                "title": {"type": "string"},
                                                "position": {"type": "object"}
                                            }
                                        },
                                        "position": {"type": "object"},
                                        "height": {"type": "number"},
                                        "width": {"type": "number"},
                                        "selected": {"type": "boolean"},
                                        "sourcePosition": {"type": "string"},
                                        "targetPosition": {"type": "string"},
                                        "type": {"type": "string"}
                                    }
                                }
                            },
                            "edges": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "required": ["id", "source", "target"],
                                    "properties": {
                                        "id": {"type": "string"},
                                        "source": {"type": "string"},
                                        "target": {"type": "string"},
                                        "sourceHandle": {"type": "string"},
                                        "targetHandle": {"type": "string"},
                                        "data": {"type": "object"},
                                        "type": {"type": "string"}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    # 节点类型验证规则
    NODE_TYPE_RULES = {
        'start': {'required_fields': ['type', 'title'], 'allowed_connections': ['outgoing']},
        'end': {'required_fields': ['type', 'title'], 'allowed_connections': ['incoming']},
        'llm': {
            'required_fields': ['type', 'title', 'model_provider', 'model_name'],
            'optional_fields': ['prompt_template', 'model_parameters', 'context'],
            'allowed_connections': ['incoming', 'outgoing']
        },
        'code': {
            'required_fields': ['type', 'title', 'code_language'],
            'optional_fields': ['code', 'variables', 'outputs'],
            'allowed_connections': ['incoming', 'outgoing']
        },
        'agent': {
            'required_fields': ['type', 'title', 'agent_strategy_provider_name', 'agent_strategy_name'],
            'optional_fields': ['agent_parameters', 'prompt_template'],
            'allowed_connections': ['incoming', 'outgoing']
        },
        'tool': {
            'required_fields': ['type', 'title', 'provider', 'tool_name'],
            'optional_fields': ['tool_parameters', 'tool_credential'],
            'allowed_connections': ['incoming', 'outgoing']
        },
        'http_request': {
            'required_fields': ['type', 'title', 'method', 'url'],
            'optional_fields': ['headers', 'params', 'body', 'authorization', 'timeout'],
            'allowed_connections': ['incoming', 'outgoing']
        },
        'if_else': {
            'required_fields': ['type', 'title', 'conditions', 'logical_operator'],
            'optional_fields': ['variable_selectors'],
            'allowed_connections': ['incoming', 'outgoing']
        },
        'iteration': {
            'required_fields': ['type', 'title', 'iterator_selector', 'start_node_id'],
            'optional_fields': ['output_selector', 'output_type'],
            'allowed_connections': ['incoming', 'outgoing']
        },
        'template_transform': {
            'required_fields': ['type', 'title', 'template'],
            'optional_fields': ['variable_selectors'],
            'allowed_connections': ['incoming', 'outgoing']
        },
        'knowledge_retrieval': {
            'required_fields': ['type', 'title', 'dataset_ids', 'retrieval_mode'],
            'optional_fields': ['query_variable_selector', 'all_datasets'],
            'allowed_connections': ['incoming', 'outgoing']
        }
    }

    @staticmethod
    def validate_dsl_structure(workflow_data: Dict) -> Tuple[bool, List[str]]:
        """验证DSL整体结构"""
        errors = []

        try:
            # 使用JSON Schema验证基础结构
            jsonschema.validate(instance=workflow_data, schema=DSLValidator.DIFY_SCHEMA)
        except ValidationError as e:
            errors.append(f"DSL结构验证失败: {e.message}")
            return False, errors

        # 验证工作流图结构
        graph_errors = DSLValidator._validate_graph_structure(workflow_data)
        errors.extend(graph_errors)

        # 验证节点连接关系
        connection_errors = DSLValidator._validate_node_connections(workflow_data)
        errors.extend(connection_errors)

        # 验证节点配置
        node_errors = DSLValidator._validate_node_configurations(workflow_data)
        errors.extend(node_errors)

        return len(errors) == 0, errors

    @staticmethod
    def _validate_graph_structure(workflow_data: Dict) -> List[str]:
        """验证图结构"""
        errors = []

        graph = workflow_data.get('workflow', {}).get('graph', {})
        nodes = graph.get('nodes', [])
        edges = graph.get('edges', [])

        # 检查是否有节点
        if not nodes:
            errors.append("工作流图中没有节点")

        # 检查是否有开始和结束节点
        node_types = [node.get('data', {}).get('type') for node in nodes]
        if 'start' not in node_types:
            errors.append("工作流缺少开始节点")
        if 'end' not in node_types:
            errors.append("工作流缺少结束节点")

        # 检查节点ID唯一性
        node_ids = [node.get('id') for node in nodes]
        if len(node_ids) != len(set(node_ids)):
            errors.append("节点ID不唯一")

        # 检查边引用有效性
        node_id_set = set(node_ids)
        for edge in edges:
            source_id = edge.get('source')
            target_id = edge.get('target')
            if source_id not in node_id_set:
                errors.append(f"边 {edge.get('id')} 引用了不存在的源节点 {source_id}")
            if target_id not in node_id_set:
                errors.append(f"边 {edge.get('id')} 引用了不存在的目标节点 {target_id}")

        return errors

    @staticmethod
    def _validate_node_connections(workflow_data: Dict) -> List[str]:
        """验证节点连接关系"""
        errors = []

        graph = workflow_data.get('workflow', {}).get('graph', {})
        nodes = graph.get('nodes', [])
        edges = graph.get('edges', [])

        # 构建节点连接图
        incoming_connections = {}
        outgoing_connections = {}

        for node in nodes:
            node_id = node.get('id')
            incoming_connections[node_id] = []
            outgoing_connections[node_id] = []

        for edge in edges:
            source_id = edge.get('source')
            target_id = edge.get('target')
            outgoing_connections[source_id].append(target_id)
            incoming_connections[target_id].append(source_id)

        # 验证连接规则
        for node in nodes:
            node_id = node.get('id')
            node_type = node.get('data', {}).get('type')

            # 开始节点不能有入边
            if node_type == 'start' and incoming_connections[node_id]:
                errors.append(f"开始节点 {node_id} 不能有入边")

            # 结束节点不能有出边
            if node_type == 'end' and outgoing_connections[node_id]:
                errors.append(f"结束节点 {node_id} 不能有出边")

            # 检查循环依赖（简化版本）
            if DSLValidator._has_circular_dependency(node_id, outgoing_connections):
                errors.append(f"节点 {node_id} 存在循环依赖")

        return errors

    @staticmethod
    def _validate_node_configurations(workflow_data: Dict) -> List[str]:
        """验证节点配置"""
        errors = []

        nodes = workflow_data.get('workflow', {}).get('graph', {}).get('nodes', [])

        for node in nodes:
            node_data = node.get('data', {})
            node_type = node_data.get('type')
            node_id = node.get('id')

            # 获取节点验证规则
            rules = DSLValidator.NODE_TYPE_RULES.get(node_type, {})

            if not rules:
                # 对于未知节点类型，给出警告但不报错
                continue

            # 检查必需字段
            required_fields = rules.get('required_fields', [])
            for field in required_fields:
                if field not in node_data:
                    errors.append(f"节点 {node_id} ({node_type}) 缺少必需字段: {field}")

            # 验证特定字段
            if node_type == 'llm':
                errors.extend(DSLValidator._validate_llm_node(node_data, node_id))
            elif node_type == 'agent':
                errors.extend(DSLValidator._validate_agent_node(node_data, node_id))
            elif node_type == 'http_request':
                errors.extend(DSLValidator._validate_http_node(node_data, node_id))

        return errors

    @staticmethod
    def _validate_llm_node(node_data: Dict, node_id: str) -> List[str]:
        """验证LLM节点配置"""
        errors = []

        # 检查模型提供商
        model_provider = node_data.get('model_provider')
        if model_provider and model_provider not in ['openai', 'anthropic', 'google', 'azure_openai']:
            errors.append(f"LLM节点 {node_id} 使用了不支持的模型提供商: {model_provider}")

        # 检查提示词模板
        prompt_template = node_data.get('prompt_template', [])
        if not isinstance(prompt_template, list):
            errors.append(f"LLM节点 {node_id} 的提示词模板必须是列表")
        else:
            for i, prompt in enumerate(prompt_template):
                if not isinstance(prompt, dict) or 'role' not in prompt or 'text' not in prompt:
                    errors.append(f"LLM节点 {node_id} 的提示词 {i} 格式不正确")

        return errors

    @staticmethod
    def _validate_agent_node(node_data: Dict, node_id: str) -> List[str]:
        """验证Agent节点配置"""
        errors = []

        # 检查策略配置
        strategy_provider = node_data.get('agent_strategy_provider_name')
        strategy_name = node_data.get('agent_strategy_name')

        if not strategy_provider:
            errors.append(f"Agent节点 {node_id} 缺少策略提供商配置")
        if not strategy_name:
            errors.append(f"Agent节点 {node_id} 缺少策略名称配置")

        # 检查Agent参数
        agent_parameters = node_data.get('agent_parameters', {})
        if agent_parameters and not isinstance(agent_parameters, dict):
            errors.append(f"Agent节点 {node_id} 的参数格式不正确")

        return errors

    @staticmethod
    def _validate_http_node(node_data: Dict, node_id: str) -> List[str]:
        """验证HTTP节点配置"""
        errors = []

        # 检查URL格式
        url = node_data.get('url', '')
        if url and not (url.startswith('http://') or url.startswith('https://')):
            errors.append(f"HTTP节点 {node_id} 的URL格式不正确: {url}")

        # 检查方法
        method = node_data.get('method', '').upper()
        valid_methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS']
        if method and method not in valid_methods:
            errors.append(f"HTTP节点 {node_id} 使用了不支持的方法: {method}")

        # 检查超时时间
        timeout = node_data.get('timeout', 60)
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            errors.append(f"HTTP节点 {node_id} 的超时时间必须是正数")

        return errors

    @staticmethod
    def _has_circular_dependency(node_id: str, outgoing_connections: Dict, visited: set = None, path: set = None) -> bool:
        """检测循环依赖（简化版本）"""
        if visited is None:
            visited = set()
        if path is None:
            path = set()

        if node_id in path:
            return True

        if node_id in visited:
            return False

        visited.add(node_id)
        path.add(node_id)

        for neighbor in outgoing_connections.get(node_id, []):
            if DSLValidator._has_circular_dependency(neighbor, outgoing_connections, visited, path):
                return True

        path.remove(node_id)
        return False


class WorkflowVersionManager:
    """工作流版本管理器 - 增强版，支持版本兼容性和迁移"""

    # Dify DSL版本兼容性矩阵
    VERSION_COMPATIBILITY = {
        "0.1.0": {"compatible_versions": ["0.1.0"], "features": ["basic_workflow"]},
        "0.1.1": {"compatible_versions": ["0.1.0", "0.1.1"], "features": ["basic_workflow", "conversation_variables"]},
        "0.1.2": {"compatible_versions": ["0.1.0", "0.1.1", "0.1.2"], "features": ["basic_workflow", "conversation_variables", "environment_variables"]},
        "0.1.3": {"compatible_versions": ["0.1.0", "0.1.1", "0.1.2", "0.1.3"], "features": ["basic_workflow", "conversation_variables", "environment_variables", "features_config"]},
        "0.1.4": {"compatible_versions": ["0.1.0", "0.1.1", "0.1.2", "0.1.3", "0.1.4"], "features": ["basic_workflow", "conversation_variables", "environment_variables", "features_config", "advanced_tools"]},
        "0.1.5": {"compatible_versions": ["0.1.0", "0.1.1", "0.1.2", "0.1.3", "0.1.4", "0.1.5"], "features": ["basic_workflow", "conversation_variables", "environment_variables", "features_config", "advanced_tools", "mcp_support"]}
    }

    # 版本迁移规则
    MIGRATION_RULES = {
        "0.1.0_to_0.1.1": {
            "add_fields": {
                "workflow.conversation_variables": []
            }
        },
        "0.1.1_to_0.1.2": {
            "add_fields": {
                "workflow.environment_variables": []
            }
        },
        "0.1.2_to_0.1.3": {
            "add_fields": {
                "workflow.features": {}
            }
        },
        "0.1.3_to_0.1.4": {
            "add_fields": {
                "dependencies": []
            }
        },
        "0.1.4_to_0.1.5": {
            "add_fields": {
                "workflow.features.mcp_enabled": False
            }
        }
    }

    @staticmethod
    def extract_version_from_filename(filename: str) -> str:
        """从文件名提取版本号 - 增强版"""
        # 匹配模式如: "AI Code Review-V4.1.yml" -> "V4.1"
        patterns = [
            r'-[Vv](\d+\.\d+)',  # -V4.1 或 -v4.1
            r'[Vv](\d+\.\d+)',   # V4.1 或 v4.1
            r'-(\d+\.\d+)',      # -4.1
            r'[_-]v?(\d+)_(\d+)', # v4_1 或 -v4_1
            r'version[_-]?(\d+\.\d+)', # version4.1
        ]

        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                if len(match.groups()) == 2:
                    # 处理 v4_1 格式
                    major, minor = match.groups()
                    return f"V{major}.{minor}"
                else:
                    return f"V{match.group(1)}"

        # 如果没有找到版本号，使用默认
        return "V1.0"

    @staticmethod
    def parse_dsl_version(workflow_data: Dict) -> str:
        """解析DSL版本号"""
        version = workflow_data.get('version', '0.1.0')
        if isinstance(version, str) and version.startswith('0.'):
            return version
        return '0.1.0'

    @staticmethod
    def check_version_compatibility(source_version: str, target_version: str) -> Tuple[bool, str]:
        """检查版本兼容性"""
        if source_version not in WorkflowVersionManager.VERSION_COMPATIBILITY:
            return False, f"不支持的源版本: {source_version}"

        if target_version not in WorkflowVersionManager.VERSION_COMPATIBILITY:
            return False, f"不支持的目标版本: {target_version}"

        source_info = WorkflowVersionManager.VERSION_COMPATIBILITY[source_version]
        compatible_versions = source_info["compatible_versions"]

        if target_version in compatible_versions:
            return True, "版本兼容"
        else:
            return False, f"版本 {source_version} 与 {target_version} 不兼容"

    @staticmethod
    def migrate_workflow(workflow_data: Dict, target_version: str) -> Tuple[bool, Dict, List[str]]:
        """迁移工作流到目标版本"""
        current_version = WorkflowVersionManager.parse_dsl_version(workflow_data)

        if current_version == target_version:
            return True, workflow_data, ["工作流已是目标版本"]

        # 检查兼容性
        is_compatible, message = WorkflowVersionManager.check_version_compatibility(current_version, target_version)
        if not is_compatible:
            return False, workflow_data, [f"版本迁移失败: {message}"]

        # 执行迁移
        migrated_data = workflow_data.copy()
        migration_log = []

        # 按顺序执行迁移步骤
        version_steps = WorkflowVersionManager._get_migration_path(current_version, target_version)

        for step in version_steps:
            rule_key = f"{step['from']}_to_{step['to']}"
            if rule_key in WorkflowVersionManager.MIGRATION_RULES:
                rule = WorkflowVersionManager.MIGRATION_RULES[rule_key]
                migrated_data, step_log = WorkflowVersionManager._apply_migration_rule(migrated_data, rule)
                migration_log.extend(step_log)

        # 更新版本号
        migrated_data['version'] = target_version
        migration_log.append(f"成功迁移到版本 {target_version}")

        return True, migrated_data, migration_log

    @staticmethod
    def _get_migration_path(from_version: str, to_version: str) -> List[Dict[str, str]]:
        """获取版本迁移路径"""
        # 简化版本：假设版本号是递增的
        from_parts = [int(x) for x in from_version.split('.')]
        to_parts = [int(x) for x in to_version.split('.')]

        if from_parts >= to_parts:
            return []

        path = []
        current = from_parts[:]

        while current < to_parts:
            next_version = current[:]
            if current[2] < 9:  # 补丁版本
                next_version[2] += 1
            elif current[1] < 9:  # 次版本
                next_version[1] += 1
                next_version[2] = 0
            else:  # 主版本
                next_version[0] += 1
                next_version[1] = 0
                next_version[2] = 0

            if next_version <= to_parts:
                path.append({
                    'from': '.'.join(map(str, current)),
                    'to': '.'.join(map(str, next_version))
                })
                current = next_version
            else:
                break

        return path

    @staticmethod
    def _apply_migration_rule(workflow_data: Dict, rule: Dict) -> Tuple[Dict, List[str]]:
        """应用迁移规则"""
        migrated_data = workflow_data.copy()
        migration_log = []

        # 添加字段
        if 'add_fields' in rule:
            for field_path, default_value in rule['add_fields'].items():
                if WorkflowVersionManager._set_nested_field(migrated_data, field_path, default_value):
                    migration_log.append(f"添加字段: {field_path}")

        return migrated_data, migration_log

    @staticmethod
    def _set_nested_field(data: Dict, field_path: str, value) -> bool:
        """设置嵌套字段值"""
        try:
            keys = field_path.split('.')
            current = data

            # 遍历到倒数第二个键
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]

            # 设置最后一个键的值
            final_key = keys[-1]
            if final_key not in current:
                current[final_key] = value
                return True

            return False  # 字段已存在
        except Exception:
            return False

    @staticmethod
    def generate_version_report(workflow_data: Dict, output_path: str = None) -> str:
        """生成版本分析报告"""
        current_version = WorkflowVersionManager.parse_dsl_version(workflow_data)
        filename_version = WorkflowVersionManager.extract_version_from_filename(
            workflow_data.get('app', {}).get('name', 'unknown')
        )

        report_lines = [
            "# 工作流版本分析报告",
            f"生成时间: {datetime.now().isoformat()}",
            "",
            "## 版本信息",
            f"- DSL版本: {current_version}",
            f"- 文件名版本: {filename_version}",
            f"- 版本一致性: {'[OK]' if current_version.replace('.', '') in filename_version else '[MISMATCH]'}",
            "",
            "## 兼容性检查",
        ]

        if current_version in WorkflowVersionManager.VERSION_COMPATIBILITY:
            version_info = WorkflowVersionManager.VERSION_COMPATIBILITY[current_version]
            compatible_versions = version_info["compatible_versions"]
            features = version_info["features"]

            report_lines.extend([
                f"- 兼容版本: {', '.join(compatible_versions)}",
                f"- 支持功能: {', '.join(features)}",
                "",
                "## 推荐操作",
            ])

            # 检查是否需要升级
            latest_version = max(WorkflowVersionManager.VERSION_COMPATIBILITY.keys())
            if current_version != latest_version:
                report_lines.append(f"- 建议升级到最新版本: {latest_version}")
            else:
                report_lines.append("- 当前已是最新版本")

        report_content = "\n".join(report_lines)

        if output_path:
            try:
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(report_content)
            except Exception as e:
                print(f"保存版本报告失败: {e}")

        return report_content
    
    @staticmethod
    def generate_output_dirname(yaml_file: str, base_name: str = "parsed_workflow") -> str:
        """生成带版本号和时间戳的输出目录名"""
        version = WorkflowVersionManager.extract_version_from_filename(yaml_file)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{base_name}_{version}_{timestamp}"
    
    @staticmethod
    def parse_dirname_info(dirname: str) -> Dict[str, str]:
        """解析目录名中的版本和时间信息"""
        # 匹配格式: parsed_workflow_V4.1_20240115_143022
        pattern = r'(.+)_(V\d+\.\d+)_(\d{8}_\d{6})'
        match = re.match(pattern, dirname)
        
        if match:
            return {
                'base_name': match.group(1),
                'version': match.group(2),
                'timestamp': match.group(3),
                'datetime': datetime.strptime(match.group(3), "%Y%m%d_%H%M%S")
            }
        return {}


class ToolManager:
    """Dify工具管理器 - 支持内置工具和MCP工具"""

    # 内置工具定义
    BUILT_IN_TOOLS = {
        'web_search': {
            'name': 'web_search',
            'description': '网页搜索工具',
            'parameters': {
                'query': {'type': 'string', 'required': True, 'description': '搜索查询'},
                'max_results': {'type': 'integer', 'required': False, 'default': 5},
                'engine': {'type': 'string', 'required': False, 'default': 'google'}
            },
            'credentials': ['api_key']
        },
        'calculator': {
            'name': 'calculator',
            'description': '计算器工具',
            'parameters': {
                'expression': {'type': 'string', 'required': True, 'description': '数学表达式'}
            }
        },
        'database_query': {
            'name': 'database_query',
            'description': '数据库查询工具',
            'parameters': {
                'query': {'type': 'string', 'required': True, 'description': 'SQL查询语句'},
                'connection_string': {'type': 'string', 'required': True, 'description': '数据库连接字符串'}
            },
            'credentials': ['db_username', 'db_password']
        },
        'file_operation': {
            'name': 'file_operation',
            'description': '文件操作工具',
            'parameters': {
                'operation': {'type': 'string', 'required': True, 'enum': ['read', 'write', 'delete', 'list']},
                'file_path': {'type': 'string', 'required': True, 'description': '文件路径'},
                'content': {'type': 'string', 'required': False, 'description': '写入内容'}
            }
        },
        'email_sender': {
            'name': 'email_sender',
            'description': '邮件发送工具',
            'parameters': {
                'to': {'type': 'string', 'required': True, 'description': '收件人邮箱'},
                'subject': {'type': 'string', 'required': True, 'description': '邮件主题'},
                'body': {'type': 'string', 'required': True, 'description': '邮件内容'}
            },
            'credentials': ['smtp_server', 'smtp_username', 'smtp_password']
        },
        'api_caller': {
            'name': 'api_caller',
            'description': 'API调用工具',
            'parameters': {
                'url': {'type': 'string', 'required': True, 'description': 'API URL'},
                'method': {'type': 'string', 'required': False, 'default': 'GET', 'enum': ['GET', 'POST', 'PUT', 'DELETE']},
                'headers': {'type': 'object', 'required': False, 'description': '请求头'},
                'body': {'type': 'object', 'required': False, 'description': '请求体'}
            },
            'credentials': ['api_key', 'bearer_token']
        }
    }

    # MCP工具类型映射
    MCP_TOOL_TYPES = {
        'mcp-server': 'Model Context Protocol Server',
        'mcp-client': 'Model Context Protocol Client',
        'mcp-resource': 'MCP Resource Access',
        'mcp-tool': 'MCP Tool Execution'
    }

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.discovered_tools = {}  # 已发现的工具
        self.tool_dependencies = {}  # 工具依赖关系

    def analyze_tools_in_workflow(self, workflow_data: Dict) -> Dict[str, Any]:
        """分析工作流中的工具使用情况"""
        tools_analysis = {
            'built_in_tools': [],
            'mcp_tools': [],
            'custom_tools': [],
            'tool_dependencies': {},
            'credentials_required': [],
            'statistics': {
                'total_tools': 0,
                'enabled_tools': 0,
                'tools_with_credentials': 0
            }
        }

        graph = workflow_data.get('workflow', {}).get('graph', {})
        nodes = graph.get('nodes', [])

        for node in nodes:
            node_data = node.get('data', {})
            node_type = node_data.get('type')

            if node_type == 'tool':
                tools_analysis['statistics']['total_tools'] += 1
                tool_config = self._analyze_tool_node(node_data, node.get('id'))
                tools_analysis['built_in_tools'].append(tool_config)

            elif node_type == 'mcp-server':
                tools_analysis['statistics']['total_tools'] += 1
                mcp_config = self._analyze_mcp_node(node_data, node.get('id'))
                tools_analysis['mcp_tools'].append(mcp_config)

            elif node_type == 'agent':
                # Agent节点可能包含工具配置
                agent_tools = self._analyze_agent_tools(node_data, node.get('id'))
                tools_analysis['custom_tools'].extend(agent_tools)
                tools_analysis['statistics']['total_tools'] += len(agent_tools)

        # 统计信息
        tools_analysis['statistics'] = self._calculate_tool_statistics(tools_analysis)

        # 分析工具依赖关系
        tools_analysis['tool_dependencies'] = self._analyze_tool_dependencies(workflow_data)

        return tools_analysis

    def _analyze_tool_node(self, node_data: Dict, node_id: str) -> Dict[str, Any]:
        """分析工具节点"""
        tool_config = {
            'node_id': node_id,
            'tool_name': node_data.get('tool_name', 'unknown'),
            'provider': node_data.get('provider', 'unknown'),
            'enabled': node_data.get('enabled', True),
            'parameters': node_data.get('tool_parameters', {}),
            'credentials': node_data.get('tool_credential', {}),
            'has_credentials': bool(node_data.get('tool_credential')),
            'is_valid': self._validate_tool_config(node_data)
        }

        # 检查是否为内置工具
        if tool_config['tool_name'] in self.BUILT_IN_TOOLS:
            built_in_def = self.BUILT_IN_TOOLS[tool_config['tool_name']]
            tool_config['is_built_in'] = True
            tool_config['description'] = built_in_def['description']
            tool_config['required_credentials'] = built_in_def.get('credentials', [])
        else:
            tool_config['is_built_in'] = False
            tool_config['description'] = '自定义工具'

        return tool_config

    def _analyze_mcp_node(self, node_data: Dict, node_id: str) -> Dict[str, Any]:
        """分析MCP节点"""
        mcp_config = {
            'node_id': node_id,
            'mcp_server_ids': node_data.get('mcp_server_ids', []),
            'mcp_tools': node_data.get('mcp_tools', []),
            'enabled': True,
            'type': 'mcp-server',
            'description': self.MCP_TOOL_TYPES.get('mcp-server', 'MCP Server'),
            'is_valid': self._validate_mcp_config(node_data)
        }

        return mcp_config

    def _analyze_agent_tools(self, node_data: Dict, node_id: str) -> List[Dict[str, Any]]:
        """分析Agent节点中的工具"""
        tools = []
        agent_parameters = node_data.get('agent_parameters', {})

        # 检查工具参数
        if isinstance(agent_parameters, dict):
            tools_param = agent_parameters.get('tools', [])
            if isinstance(tools_param, list):
                for i, tool in enumerate(tools_param):
                    tool_config = {
                        'node_id': f"{node_id}_tool_{i}",
                        'tool_name': tool.get('name', f'agent_tool_{i}'),
                        'provider': 'agent',
                        'enabled': tool.get('enabled', True),
                        'parameters': tool.get('parameters', {}),
                        'settings': tool.get('settings', {}),
                        'is_built_in': False,
                        'description': f"Agent工具: {tool.get('name', 'unknown')}",
                        'has_credentials': bool(tool.get('settings')),
                        'is_valid': True
                    }
                    tools.append(tool_config)

        return tools

    def _validate_tool_config(self, node_data: Dict) -> bool:
        """验证工具配置"""
        try:
            tool_name = node_data.get('tool_name')
            if not tool_name:
                return False

            # 如果是内置工具，进行更严格的验证
            if tool_name in self.BUILT_IN_TOOLS:
                built_in_def = self.BUILT_IN_TOOLS[tool_name]
                required_params = [k for k, v in built_in_def['parameters'].items() if v.get('required', False)]

                tool_params = node_data.get('tool_parameters', {})
                for param in required_params:
                    if param not in tool_params:
                        self.logger.warning(f"工具 {tool_name} 缺少必需参数: {param}")
                        return False

            return True
        except Exception as e:
            self.logger.error(f"工具配置验证失败: {e}")
            return False

    def _validate_mcp_config(self, node_data: Dict) -> bool:
        """验证MCP配置"""
        try:
            mcp_server_ids = node_data.get('mcp_server_ids', [])
            if not mcp_server_ids:
                self.logger.warning("MCP节点缺少服务器ID配置")
                return False

            # 检查MCP服务器ID格式
            for server_id in mcp_server_ids:
                if not isinstance(server_id, str) or not server_id.strip():
                    return False

            return True
        except Exception as e:
            self.logger.error(f"MCP配置验证失败: {e}")
            return False

    def _calculate_tool_statistics(self, tools_analysis: Dict) -> Dict[str, int]:
        """计算工具统计信息"""
        # 计算总工具数量
        total_tools = (len(tools_analysis.get('built_in_tools', [])) +
                      len(tools_analysis.get('mcp_tools', [])) +
                      len(tools_analysis.get('custom_tools', [])))

        stats = {
            'total_tools': total_tools,
            'enabled_tools': 0,
            'tools_with_credentials': 0,
            'built_in_tools_count': len(tools_analysis.get('built_in_tools', [])),
            'mcp_tools_count': len(tools_analysis.get('mcp_tools', [])),
            'custom_tools_count': len(tools_analysis.get('custom_tools', []))
        }

        # 计算启用工具数量
        for tool in tools_analysis.get('built_in_tools', []):
            if tool.get('enabled', True):
                stats['enabled_tools'] += 1
            if tool.get('has_credentials'):
                stats['tools_with_credentials'] += 1

        for tool in tools_analysis.get('custom_tools', []):
            if tool.get('enabled', True):
                stats['enabled_tools'] += 1
            if tool.get('has_credentials'):
                stats['tools_with_credentials'] += 1

        # MCP工具默认启用
        stats['enabled_tools'] += len(tools_analysis.get('mcp_tools', []))

        return stats

    def _analyze_tool_dependencies(self, workflow_data: Dict) -> Dict[str, List[str]]:
        """分析工具依赖关系"""
        dependencies = {}

        graph = workflow_data.get('workflow', {}).get('graph', {})
        nodes = graph.get('nodes', [])
        edges = graph.get('edges', [])

        # 构建节点依赖图
        for edge in edges:
            source_id = edge.get('source')
            target_id = edge.get('target')

            if source_id not in dependencies:
                dependencies[source_id] = []
            dependencies[source_id].append(target_id)

        # 识别工具相关的依赖
        tool_dependencies = {}
        for node in nodes:
            node_id = node.get('id')
            node_type = node.get('data', {}).get('type')

            if node_type in ['tool', 'mcp-server', 'agent']:
                tool_dependencies[node_id] = dependencies.get(node_id, [])

        return tool_dependencies

    def generate_tool_report(self, tools_analysis: Dict, output_path: str = None) -> str:
        """生成工具使用报告"""
        report_lines = [
            "# Dify工作流工具分析报告",
            f"生成时间: {datetime.now().isoformat()}",
            "",
            "## 工具统计",
            f"- 总工具数量: {tools_analysis['statistics']['total_tools']}",
            f"- 启用工具数量: {tools_analysis['statistics']['enabled_tools']}",
            f"- 内置工具数量: {tools_analysis['statistics']['built_in_tools_count']}",
            f"- MCP工具数量: {tools_analysis['statistics']['mcp_tools_count']}",
            f"- 自定义工具数量: {tools_analysis['statistics']['custom_tools_count']}",
            f"- 需要凭据的工具: {tools_analysis['statistics']['tools_with_credentials']}",
            "",
            "## 内置工具详情",
            ""
        ]

        for tool in tools_analysis.get('built_in_tools', []):
            status = "[ENABLED]" if tool.get('enabled') else "[DISABLED]"
            cred_status = "🔐 有凭据" if tool.get('has_credentials') else "🔓 无凭据"
            valid_status = "[VALID]" if tool.get('is_valid') else "[INVALID]"

            report_lines.extend([
                f"### {tool['tool_name']} ({tool['node_id']})",
                f"- 状态: {status}",
                f"- 凭据: {cred_status}",
                f"- 配置: {valid_status}",
                f"- 提供商: {tool['provider']}",
                f"- 描述: {tool['description']}",
                ""
            ])

        if tools_analysis.get('mcp_tools'):
            report_lines.extend([
                "## MCP工具详情",
                ""
            ])

            for tool in tools_analysis['mcp_tools']:
                valid_status = "[VALID]" if tool.get('is_valid') else "[INVALID]"
                report_lines.extend([
                    f"### MCP Server ({tool['node_id']})",
                    f"- 配置: {valid_status}",
                    f"- 服务器IDs: {', '.join(tool['mcp_server_ids'])}",
                    f"- 工具数量: {len(tool.get('mcp_tools', []))}",
                    ""
                ])

        if tools_analysis.get('custom_tools'):
            report_lines.extend([
                "## 自定义工具详情",
                ""
            ])

            for tool in tools_analysis['custom_tools']:
                status = "[ENABLED]" if tool.get('enabled') else "[DISABLED]"
                cred_status = "🔐 有凭据" if tool.get('has_credentials') else "🔓 无凭据"

                report_lines.extend([
                    f"### {tool['tool_name']} ({tool['node_id']})",
                    f"- 状态: {status}",
                    f"- 凭据: {cred_status}",
                    f"- 提供商: {tool['provider']}",
                    f"- 描述: {tool['description']}",
                    ""
                ])

        report_content = "\n".join(report_lines)

        if output_path:
            try:
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(report_content)
                self.logger.info(f"工具报告已保存到: {output_path}")
            except Exception as e:
                self.logger.error(f"保存工具报告失败: {e}")

        return report_content


class WorkflowParallelProcessor:
    """工作流并行处理器"""

    def __init__(self, max_workers: int = None):
        self.max_workers = max_workers or min(8, os.cpu_count() or 4)
        self.logger = logging.getLogger(__name__)

    def analyze_dependencies(self, workflow_data: Dict) -> Dict[str, List[str]]:
        """分析节点依赖关系

        Returns:
            节点依赖图：{node_id: [dependent_node_ids]}
        """
        graph = workflow_data.get('workflow', {}).get('graph', {})
        edges = graph.get('edges', [])

        # 构建依赖图：node_id -> [依赖此节点的下游节点]
        dependencies = {}

        for edge in edges:
            source_id = edge.get('source')
            target_id = edge.get('target')

            if source_id not in dependencies:
                dependencies[source_id] = []
            if target_id not in dependencies:
                dependencies[target_id] = []

            # target依赖source，所以source -> target
            dependencies[source_id].append(target_id)

        # 确保所有节点都在依赖图中
        nodes = graph.get('nodes', [])
        for node in nodes:
            node_id = node.get('id')
            if node_id not in dependencies:
                dependencies[node_id] = []

        return dependencies

    def get_execution_levels(self, dependencies: Dict[str, List[str]]) -> List[List[str]]:
        """获取执行层级（拓扑排序）

        Returns:
            执行层级列表，每个层级包含可以并行执行的节点
        """
        # 计算入度
        indegree = {node: 0 for node in dependencies}
        for node in dependencies:
            for dependent in dependencies[node]:
                indegree[dependent] += 1

        # 拓扑排序
        levels = []
        current_level = [node for node in indegree if indegree[node] == 0]

        while current_level:
            levels.append(current_level[:])  # 复制当前层级

            next_level = []
            for node in current_level:
                for dependent in dependencies[node]:
                    indegree[dependent] -= 1
                    if indegree[dependent] == 0:
                        next_level.append(dependent)

            current_level = next_level

        return levels

    def process_nodes_parallel(self, nodes: List[Dict], process_func, **kwargs) -> Dict[str, Any]:
        """并行处理节点

        Args:
            nodes: 节点列表
            process_func: 处理函数，接收(node, **kwargs)
            **kwargs: 传递给处理函数的参数

        Returns:
            {node_id: result}
        """
        results = {}
        lock = threading.Lock()

        def process_single_node(node):
            try:
                result = process_func(node, **kwargs)
                with lock:
                    results[node['id']] = result
                return node['id'], result
            except Exception as e:
                self.logger.error(f"处理节点 {node['id']} 失败: {e}")
                with lock:
                    results[node['id']] = None
                return node['id'], None

        # 使用线程池并行处理
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(process_single_node, node) for node in nodes]

            for future in as_completed(futures):
                try:
                    node_id, result = future.result()
                    if result is None:
                        self.logger.warning(f"节点 {node_id} 处理失败")
                except Exception as e:
                    self.logger.error(f"节点处理任务失败: {e}")

        return results


class WorkflowSplitter:
    """工作流拆分器 - 增强版，支持原始格式保留和位置映射"""
    
    def __init__(self, yaml_file: str, output_dir: str = None, enable_parallel: bool = True):
        self.yaml_file = Path(yaml_file)
        if output_dir is None:
            output_dir = WorkflowVersionManager.generate_output_dirname(str(yaml_file))
        self.output_dir = Path(output_dir)

        self.workflow_data = None
        self.raw_yaml_lines = []  # 原始YAML行
        self.node_positions = {}  # 节点位置映射
        self.metadata = {}

        # 并行处理配置
        self.enable_parallel = enable_parallel
        self.parallel_processor = WorkflowParallelProcessor() if enable_parallel else None

        # 错误处理
        self.error_handler = ErrorHandler()

        # 工具管理
        self.tool_manager = ToolManager()

        # 设置日志
        self._setup_logging()

        # 初始化YAML处理器
        if YAML_AVAILABLE:
            self.yaml_processor = YAML()
            self.yaml_processor.preserve_quotes = True
            self.yaml_processor.width = 4096
        else:
            self.yaml_processor = None
    
    def _setup_logging(self):
        """设置日志"""
        log_dir = self.output_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(log_dir / 'workflow_manager.log', encoding='utf-8')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def split_workflow(self, validate_dsl: bool = True) -> bool:
        """拆分工作流

        Args:
            validate_dsl: 是否验证DSL结构
        """
        def _split_operation():
            self.logger.info(f"开始拆分工作流: {self.yaml_file}")

            # 读取和解析YAML
            if not self._load_yaml():
                raise Exception("YAML文件加载失败")

            # DSL结构验证
            if validate_dsl:
                self.logger.info("开始验证DSL结构...")
                is_valid, validation_errors = DSLValidator.validate_dsl_structure(self.workflow_data)
                if not is_valid:
                    error_msg = f"DSL结构验证失败: {', '.join(validation_errors)}"
                    self.logger.error(error_msg)
                    raise Exception(error_msg)
                self.logger.info("DSL结构验证通过")

            # 创建输出结构
            self._create_output_structure()

            # 生成元数据
            self._generate_metadata()

            # 拆分节点
            success = self._split_nodes()
            if not success:
                raise Exception("节点拆分失败")

            # 保存元数据和映射信息
            self._save_metadata()

            self.logger.info(f"工作流拆分完成: {self.output_dir}")
            return True

        try:
            return self.error_handler.handle_operation(_split_operation, "工作流拆分")
        except Exception as e:
            self.logger.error(f"工作流拆分失败: {e}")
            # 生成错误报告
            error_report = self.error_handler.generate_error_report(
                self.output_dir / "logs" / "error_report.md" if self.output_dir.exists() else None
            )
            if error_report != "无错误记录":
                self.logger.info("错误报告已生成")
            return False
    
    def _load_yaml(self) -> bool:
        """加载和解析YAML文件"""
        try:
            # 读取原始文本行（用于位置映射）
            with open(self.yaml_file, 'r', encoding='utf-8') as f:
                self.raw_yaml_lines = f.readlines()
            
            # 解析YAML结构
            if YAML_AVAILABLE:
                with open(self.yaml_file, 'r', encoding='utf-8') as f:
                    self.workflow_data = self.yaml_processor.load(f)
            else:
                with open(self.yaml_file, 'r', encoding='utf-8') as f:
                    self.workflow_data = yaml.safe_load(f)
            
            # 提取基本信息
            workflow_graph = self.workflow_data.get('workflow', {}).get('graph', {})
            nodes = workflow_graph.get('nodes', [])
            edges = workflow_graph.get('edges', [])
            
            self.logger.info(f"YAML解析成功: {len(nodes)} 个节点，{len(edges)} 个连接")
            return True
            
        except Exception as e:
            self.logger.error(f"YAML解析失败: {e}")
            return False
    
    def _create_output_structure(self):
        """创建输出目录结构"""
        # 主目录
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 子目录
        (self.output_dir / "metadata").mkdir(exist_ok=True)
        (self.output_dir / "nodes").mkdir(exist_ok=True)
        (self.output_dir / "tools").mkdir(exist_ok=True)
        (self.output_dir / "logs").mkdir(exist_ok=True)
        
        # 节点类型目录 - 基于Dify支持的40+种节点类型
        node_types = [
            # 基础节点
            'start', 'end', 'answer',

            # 数据处理节点
            'llm', 'code', 'template_transform', 'variable_aggregator',
            'variable_assigner', 'knowledge_retrieval',

            # 逻辑控制节点
            'if_else', 'iteration', 'iteration_start', 'loop',

            # 外部集成节点
            'http_request', 'tool', 'agent', 'question_classifier',
            'mcp_server', 'webhook', 'scheduled',

            # 触发器节点
            'trigger_webhook', 'trigger_schedule', 'trigger_plugin',

            # 其他节点
            'parameter_extractor', 'list_filter', 'doc_extractor',
            'unknown'
        ]
        
        for node_type in node_types:
            (self.output_dir / "nodes" / node_type).mkdir(exist_ok=True)
    
    def _generate_metadata(self):
        """生成元数据信息"""
        app_info = self.workflow_data.get('app', {})
        workflow_info = self.workflow_data.get('workflow', {})

        # 分析工具使用情况
        tools_analysis = self.tool_manager.analyze_tools_in_workflow(self.workflow_data)

        self.metadata = {
            'source_file': str(self.yaml_file),
            'version_info': {
                'extracted_version': WorkflowVersionManager.extract_version_from_filename(self.yaml_file.name),
                'dsl_version': WorkflowVersionManager.parse_dsl_version(self.workflow_data),
                'app_name': app_info.get('name', 'unknown'),
                'app_mode': app_info.get('mode', 'unknown'),
                'compatibility_check': WorkflowVersionManager.check_version_compatibility(
                    WorkflowVersionManager.parse_dsl_version(self.workflow_data),
                    WorkflowVersionManager.parse_dsl_version(self.workflow_data)
                )
            },
            'split_info': {
                'timestamp': datetime.now().isoformat(),
                'output_directory': str(self.output_dir),
                'total_lines': len(self.raw_yaml_lines)
            },
            'statistics': {
                'total_nodes': 0,
                'total_edges': 0,
                'node_types': {}
            },
            'tools_analysis': tools_analysis
        }
    
    def _split_nodes(self) -> bool:
        """拆分节点 - 支持并行处理"""
        try:
            workflow_graph = self.workflow_data.get('workflow', {}).get('graph', {})
            nodes = workflow_graph.get('nodes', [])

            node_stats = {}

            if self.enable_parallel and len(nodes) > 3:  # 只有在节点较多时才使用并行处理
                self.logger.info("使用并行处理拆分节点...")

                # 并行处理节点
                def process_node_wrapper(node_with_index):
                    node, i = node_with_index
                    node_data = node.get('data', {})
                    node_type = node_data.get('type', 'unknown')
                    node_id = node.get('id', f'unknown_{i}')
                    node_title = node_data.get('title', f'节点_{i}')

                    # 拆分节点
                    success = self._split_single_node(node, node_type, i + 1)  # 使用实际索引+1
                    return {
                        'node_id': node_id,
                        'node_type': node_type,
                        'success': success,
                        'index': i
                    }

                # 准备节点数据
                nodes_with_indices = [(node, i) for i, node in enumerate(nodes)]

                # 并行处理
                results = self.parallel_processor.process_nodes_parallel(
                    nodes_with_indices,
                    lambda node_with_index, **kwargs: process_node_wrapper(node_with_index)
                )

                # 统计结果
                for node_id, result in results.items():
                    if result and result['success']:
                        node_stats[result['node_type']] = node_stats.get(result['node_type'], 0) + 1
                    else:
                        self.logger.warning(f"节点拆分失败: {node_id}")

            else:
                # 串行处理
                self.logger.info("使用串行处理拆分节点...")
                for i, node in enumerate(nodes):
                    node_data = node.get('data', {})
                    node_type = node_data.get('type', 'unknown')
                    node_id = node.get('id', f'unknown_{i}')
                    node_title = node_data.get('title', f'节点_{i}')

                    # 统计
                    node_stats[node_type] = node_stats.get(node_type, 0) + 1
                    current_index = node_stats[node_type]

                    # 拆分节点
                    success = self._split_single_node(node, node_type, current_index)
                    if not success:
                        self.logger.warning(f"节点拆分失败: {node_id}")

            # 更新统计信息
            self.metadata['statistics']['total_nodes'] = len(nodes)
            self.metadata['statistics']['total_edges'] = len(workflow_graph.get('edges', []))
            self.metadata['statistics']['node_types'] = node_stats
            self.metadata['statistics']['parallel_processing'] = self.enable_parallel

            return True

        except Exception as e:
            self.logger.error(f"拆分节点时发生错误: {e}")
            return False
    
    def _split_single_node(self, node: Dict, node_type: str, index: int) -> bool:
        """拆分单个节点"""
        try:
            node_data = node.get('data', {})
            node_id = node.get('id', 'unknown')
            
            # 创建节点目录
            node_dir_name = f"{index:02d}_{node_type}_{node_id}"
            node_dir = self.output_dir / "nodes" / node_type / node_dir_name
            node_dir.mkdir(parents=True, exist_ok=True)
            
            # 生成节点文件 - 支持更多Dify节点类型
            if node_type == 'llm':
                return self._split_llm_node(node, node_dir)
            elif node_type == 'code':
                return self._split_code_node(node, node_dir)
            elif node_type in ['mcp-server', 'mcp_server']:
                return self._split_mcp_node(node, node_dir)
            elif node_type == 'agent':
                return self._split_agent_node(node, node_dir)
            elif node_type == 'tool':
                return self._split_tool_node(node, node_dir)
            elif node_type == 'http_request':
                return self._split_http_request_node(node, node_dir)
            elif node_type == 'if_else':
                return self._split_if_else_node(node, node_dir)
            elif node_type == 'iteration':
                return self._split_iteration_node(node, node_dir)
            elif node_type == 'template_transform':
                return self._split_template_transform_node(node, node_dir)
            elif node_type in ['start', 'end']:
                return self._split_flow_control_node(node, node_dir)
            elif node_type.startswith('trigger_'):
                return self._split_trigger_node(node, node_dir)
            else:
                return self._split_generic_node(node, node_dir)
                
        except Exception as e:
            self.logger.error(f"拆分单个节点失败: {e}")
            return False
    
    def _split_llm_node(self, node: Dict, node_dir: Path) -> bool:
        """拆分LLM节点 - 保留原始格式"""
        try:
            node_data = node.get('data', {})
            node_id = node.get('id', 'unknown')
            
            # 基础配置（去除提示词）
            base_config = {k: v for k, v in node_data.items() 
                          if k not in ['prompt_template']}
            
            # 保存基础配置
            with open(node_dir / 'config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(base_config, f)
                else:
                    yaml.dump(base_config, f, default_flow_style=False, allow_unicode=True)
            
            # 保存提示词（原始格式）
            prompt_template = node_data.get('prompt_template', [])
            for i, prompt in enumerate(prompt_template):
                role = prompt.get('role', 'user')
                text = prompt.get('text', '')
                
                # 保存为纯文本文件，保留原始格式
                prompt_file = node_dir / f'{role}_prompt_{i+1}.txt'
                with open(prompt_file, 'w', encoding='utf-8') as f:
                    f.write(text)
            
            # 保存节点元数据
            metadata = {
                'node_id': node_id,
                'node_type': 'llm',
                'title': node_data.get('title', ''),
                'position': node.get('position', {}),
                'prompt_count': len(prompt_template),
                'prompt_files': [f'{p.get("role", "user")}_prompt_{i+1}.txt' 
                               for i, p in enumerate(prompt_template)]
            }
            
            with open(node_dir / 'metadata.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            # 记录位置映射
            self.node_positions[node_id] = {
                'node_dir': str(node_dir.relative_to(self.output_dir)),
                'node_type': 'llm',
                'index_in_yaml': None  # 将在后续版本中实现行号追踪
            }
            
            return True
            
        except Exception as e:
            self.logger.error(f"拆分LLM节点失败: {e}")
            return False
    
    def _split_code_node(self, node: Dict, node_dir: Path) -> bool:
        """拆分代码节点 - 保留原始格式"""
        try:
            node_data = node.get('data', {})
            node_id = node.get('id', 'unknown')
            
            # 基础配置（去除代码）
            base_config = {k: v for k, v in node_data.items() 
                          if k not in ['code']}
            
            # 保存基础配置
            with open(node_dir / 'config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(base_config, f)
                else:
                    yaml.dump(base_config, f, default_flow_style=False, allow_unicode=True)
            
            # 保存代码（原始格式）
            code_content = node_data.get('code', '')
            code_language = node_data.get('code_language', 'python3')
            
            # 根据语言确定文件扩展名
            extension_map = {
                'python3': 'py',
                'python': 'py', 
                'javascript': 'js',
                'typescript': 'ts'
            }
            ext = extension_map.get(code_language, 'txt')
            
            with open(node_dir / f'code.{ext}', 'w', encoding='utf-8') as f:
                f.write(code_content)
            
            # 保存节点元数据
            metadata = {
                'node_id': node_id,
                'node_type': 'code',
                'title': node_data.get('title', ''),
                'code_language': code_language,
                'code_file': f'code.{ext}',
                'position': node.get('position', {})
            }
            
            with open(node_dir / 'metadata.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            # 记录位置映射
            self.node_positions[node_id] = {
                'node_dir': str(node_dir.relative_to(self.output_dir)),
                'node_type': 'code',
                'index_in_yaml': None
            }
            
            return True
            
        except Exception as e:
            self.logger.error(f"拆分代码节点失败: {e}")
            return False
    
    def _split_mcp_node(self, node: Dict, node_dir: Path) -> bool:
        """拆分MCP节点"""
        try:
            node_data = node.get('data', {})
            node_id = node.get('id', 'unknown')
            
            # 保存完整配置
            with open(node_dir / 'config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(node_data, f)
                else:
                    yaml.dump(node_data, f, default_flow_style=False, allow_unicode=True)
            
            # 保存节点元数据
            metadata = {
                'node_id': node_id,
                'node_type': 'mcp-server',
                'title': node_data.get('title', ''),
                'mcp_server_ids': node_data.get('mcp_server_ids', []),
                'position': node.get('position', {})
            }
            
            with open(node_dir / 'metadata.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            # 记录位置映射
            self.node_positions[node_id] = {
                'node_dir': str(node_dir.relative_to(self.output_dir)),
                'node_type': 'mcp-server',
                'index_in_yaml': None
            }
            
            return True
            
        except Exception as e:
            self.logger.error(f"拆分MCP节点失败: {e}")
            return False
    
    def _split_agent_node(self, node: Dict, node_dir: Path) -> bool:
        """拆分Agent节点 - 基于Dify Agent节点特性"""
        try:
            node_data = node.get('data', {})
            node_id = node.get('id', 'unknown')

            # 基础配置（去除复杂参数）
            base_config = {k: v for k, v in node_data.items()
                          if k not in ['agent_parameters', 'prompt_template']}

            # 保存基础配置
            with open(node_dir / 'config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(base_config, f)
                else:
                    yaml.dump(base_config, f, default_flow_style=False, allow_unicode=True)

            # 保存Agent参数
            agent_parameters = node_data.get('agent_parameters', {})
            if agent_parameters:
                with open(node_dir / 'agent_parameters.yaml', 'w', encoding='utf-8') as f:
                    if YAML_AVAILABLE:
                        self.yaml_processor.dump(agent_parameters, f)
                    else:
                        yaml.dump(agent_parameters, f, default_flow_style=False, allow_unicode=True)

            # 保存提示词模板
            prompt_template = node_data.get('prompt_template', [])
            for i, prompt in enumerate(prompt_template):
                role = prompt.get('role', 'user')
                text = prompt.get('text', '')

                prompt_file = node_dir / f'{role}_prompt_{i+1}.txt'
                with open(prompt_file, 'w', encoding='utf-8') as f:
                    f.write(text)

            # 保存节点元数据
            metadata = {
                'node_id': node_id,
                'node_type': 'agent',
                'title': node_data.get('title', ''),
                'agent_strategy_provider': node_data.get('agent_strategy_provider_name', ''),
                'agent_strategy': node_data.get('agent_strategy_name', ''),
                'position': node.get('position', {}),
                'prompt_count': len(prompt_template),
                'agent_parameters_file': 'agent_parameters.yaml' if agent_parameters else None,
                'prompt_files': [f'{p.get("role", "user")}_prompt_{i+1}.txt'
                               for i, p in enumerate(prompt_template)]
            }

            with open(node_dir / 'metadata.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            # 记录位置映射
            self.node_positions[node_id] = {
                'node_dir': str(node_dir.relative_to(self.output_dir)),
                'node_type': 'agent',
                'index_in_yaml': None
            }

            return True

        except Exception as e:
            self.logger.error(f"拆分Agent节点失败: {e}")
            return False

    def _split_tool_node(self, node: Dict, node_dir: Path) -> bool:
        """拆分工具节点"""
        try:
            node_data = node.get('data', {})
            node_id = node.get('id', 'unknown')

            # 工具配置
            tool_config = {
                'provider': node_data.get('provider', ''),
                'tool_name': node_data.get('tool_name', ''),
                'tool_parameters': node_data.get('tool_parameters', {}),
                'tool_credential': node_data.get('tool_credential', {})
            }

            # 保存工具配置
            with open(node_dir / 'tool_config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(tool_config, f)
                else:
                    yaml.dump(tool_config, f, default_flow_style=False, allow_unicode=True)

            # 保存基础配置（去除工具相关参数）
            base_config = {k: v for k, v in node_data.items()
                          if k not in ['tool_parameters', 'tool_credential']}
            with open(node_dir / 'config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(base_config, f)
                else:
                    yaml.dump(base_config, f, default_flow_style=False, allow_unicode=True)

            # 保存节点元数据
            metadata = {
                'node_id': node_id,
                'node_type': 'tool',
                'title': node_data.get('title', ''),
                'provider': tool_config['provider'],
                'tool_name': tool_config['tool_name'],
                'position': node.get('position', {}),
                'config_files': ['tool_config.yaml', 'config.yaml']
            }

            with open(node_dir / 'metadata.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            # 记录位置映射
            self.node_positions[node_id] = {
                'node_dir': str(node_dir.relative_to(self.output_dir)),
                'node_type': 'tool',
                'index_in_yaml': None
            }

            return True

        except Exception as e:
            self.logger.error(f"拆分工具节点失败: {e}")
            return False

    def _split_http_request_node(self, node: Dict, node_dir: Path) -> bool:
        """拆分HTTP请求节点"""
        try:
            node_data = node.get('data', {})
            node_id = node.get('id', 'unknown')

            # HTTP请求配置
            http_config = {
                'method': node_data.get('method', 'GET'),
                'url': node_data.get('url', ''),
                'headers': node_data.get('headers', []),
                'params': node_data.get('params', []),
                'body': node_data.get('body', {}),
                'authorization': node_data.get('authorization', {}),
                'timeout': node_data.get('timeout', 60)
            }

            # 保存HTTP配置
            with open(node_dir / 'http_config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(http_config, f)
                else:
                    yaml.dump(http_config, f, default_flow_style=False, allow_unicode=True)

            # 保存基础配置
            base_config = {k: v for k, v in node_data.items()
                          if k not in ['headers', 'params', 'body', 'authorization']}
            with open(node_dir / 'config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(base_config, f)
                else:
                    yaml.dump(base_config, f, default_flow_style=False, allow_unicode=True)

            # 保存节点元数据
            metadata = {
                'node_id': node_id,
                'node_type': 'http_request',
                'title': node_data.get('title', ''),
                'method': http_config['method'],
                'url': http_config['url'],
                'position': node.get('position', {}),
                'config_files': ['http_config.yaml', 'config.yaml']
            }

            with open(node_dir / 'metadata.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            # 记录位置映射
            self.node_positions[node_id] = {
                'node_dir': str(node_dir.relative_to(self.output_dir)),
                'node_type': 'http_request',
                'index_in_yaml': None
            }

            return True

        except Exception as e:
            self.logger.error(f"拆分HTTP请求节点失败: {e}")
            return False

    def _split_if_else_node(self, node: Dict, node_dir: Path) -> bool:
        """拆分条件分支节点"""
        try:
            node_data = node.get('data', {})
            node_id = node.get('id', 'unknown')

            # 条件逻辑配置
            logic_config = {
                'conditions': node_data.get('conditions', []),
                'logical_operator': node_data.get('logical_operator', 'and'),
                'variable_selectors': node_data.get('variable_selectors', [])
            }

            # 保存条件配置
            with open(node_dir / 'logic_config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(logic_config, f)
                else:
                    yaml.dump(logic_config, f, default_flow_style=False, allow_unicode=True)

            # 保存基础配置
            base_config = {k: v for k, v in node_data.items()
                          if k not in ['conditions', 'variable_selectors']}
            with open(node_dir / 'config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(base_config, f)
                else:
                    yaml.dump(base_config, f, default_flow_style=False, allow_unicode=True)

            # 保存节点元数据
            metadata = {
                'node_id': node_id,
                'node_type': 'if_else',
                'title': node_data.get('title', ''),
                'conditions_count': len(logic_config['conditions']),
                'logical_operator': logic_config['logical_operator'],
                'position': node.get('position', {}),
                'config_files': ['logic_config.yaml', 'config.yaml']
            }

            with open(node_dir / 'metadata.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            # 记录位置映射
            self.node_positions[node_id] = {
                'node_dir': str(node_dir.relative_to(self.output_dir)),
                'node_type': 'if_else',
                'index_in_yaml': None
            }

            return True

        except Exception as e:
            self.logger.error(f"拆分条件分支节点失败: {e}")
            return False

    def _split_iteration_node(self, node: Dict, node_dir: Path) -> bool:
        """拆分迭代节点"""
        try:
            node_data = node.get('data', {})
            node_id = node.get('id', 'unknown')

            # 迭代配置
            iteration_config = {
                'iterator_selector': node_data.get('iterator_selector', []),
                'output_selector': node_data.get('output_selector', []),
                'start_node_id': node_data.get('start_node_id', ''),
                'output_type': node_data.get('output_type', 'array')
            }

            # 保存迭代配置
            with open(node_dir / 'iteration_config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(iteration_config, f)
                else:
                    yaml.dump(iteration_config, f, default_flow_style=False, allow_unicode=True)

            # 保存基础配置
            base_config = {k: v for k, v in node_data.items()
                          if k not in ['iterator_selector', 'output_selector']}
            with open(node_dir / 'config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(base_config, f)
                else:
                    yaml.dump(base_config, f, default_flow_style=False, allow_unicode=True)

            # 保存节点元数据
            metadata = {
                'node_id': node_id,
                'node_type': 'iteration',
                'title': node_data.get('title', ''),
                'start_node_id': iteration_config['start_node_id'],
                'output_type': iteration_config['output_type'],
                'position': node.get('position', {}),
                'config_files': ['iteration_config.yaml', 'config.yaml']
            }

            with open(node_dir / 'metadata.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            # 记录位置映射
            self.node_positions[node_id] = {
                'node_dir': str(node_dir.relative_to(self.output_dir)),
                'node_type': 'iteration',
                'index_in_yaml': None
            }

            return True

        except Exception as e:
            self.logger.error(f"拆分迭代节点失败: {e}")
            return False

    def _split_template_transform_node(self, node: Dict, node_dir: Path) -> bool:
        """拆分模板转换节点"""
        try:
            node_data = node.get('data', {})
            node_id = node.get('id', 'unknown')

            # 模板配置
            template_config = {
                'template': node_data.get('template', ''),
                'variable_selectors': node_data.get('variable_selectors', [])
            }

            # 保存模板内容
            with open(node_dir / 'template.txt', 'w', encoding='utf-8') as f:
                f.write(template_config['template'])

            # 保存变量选择器配置
            with open(node_dir / 'variable_selectors.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(template_config['variable_selectors'], f)
                else:
                    yaml.dump(template_config['variable_selectors'], f, default_flow_style=False, allow_unicode=True)

            # 保存基础配置
            base_config = {k: v for k, v in node_data.items()
                          if k not in ['template', 'variable_selectors']}
            with open(node_dir / 'config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(base_config, f)
                else:
                    yaml.dump(base_config, f, default_flow_style=False, allow_unicode=True)

            # 保存节点元数据
            metadata = {
                'node_id': node_id,
                'node_type': 'template_transform',
                'title': node_data.get('title', ''),
                'template_file': 'template.txt',
                'variable_selectors_file': 'variable_selectors.yaml',
                'position': node.get('position', {}),
                'config_files': ['config.yaml', 'template.txt', 'variable_selectors.yaml']
            }

            with open(node_dir / 'metadata.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            # 记录位置映射
            self.node_positions[node_id] = {
                'node_dir': str(node_dir.relative_to(self.output_dir)),
                'node_type': 'template_transform',
                'index_in_yaml': None
            }

            return True

        except Exception as e:
            self.logger.error(f"拆分模板转换节点失败: {e}")
            return False

    def _split_flow_control_node(self, node: Dict, node_dir: Path) -> bool:
        """拆分流程控制节点（start/end）"""
        try:
            node_data = node.get('data', {})
            node_id = node.get('id', 'unknown')
            node_type = node_data.get('type', 'unknown')

            # 保存完整配置
            with open(node_dir / 'config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(node_data, f)
                else:
                    yaml.dump(node_data, f, default_flow_style=False, allow_unicode=True)

            # 保存节点元数据
            metadata = {
                'node_id': node_id,
                'node_type': node_type,
                'title': node_data.get('title', ''),
                'position': node.get('position', {}),
                'is_flow_control': True,
                'flow_role': node_type  # start 或 end
            }

            with open(node_dir / 'metadata.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            # 记录位置映射
            self.node_positions[node_id] = {
                'node_dir': str(node_dir.relative_to(self.output_dir)),
                'node_type': node_type,
                'index_in_yaml': None
            }

            return True

        except Exception as e:
            self.logger.error(f"拆分流程控制节点失败: {e}")
            return False

    def _split_trigger_node(self, node: Dict, node_dir: Path) -> bool:
        """拆分触发器节点"""
        try:
            node_data = node.get('data', {})
            node_id = node.get('id', 'unknown')
            node_type = node_data.get('type', 'unknown')

            # 触发器配置
            trigger_config = {
                'trigger_type': node_type.replace('trigger_', ''),
                'config': node_data.get('config', {}),
                'schedule_config': node_data.get('schedule_config', {}),
                'webhook_config': node_data.get('webhook_config', {})
            }

            # 保存触发器配置
            with open(node_dir / 'trigger_config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(trigger_config, f)
                else:
                    yaml.dump(trigger_config, f, default_flow_style=False, allow_unicode=True)

            # 保存基础配置
            base_config = {k: v for k, v in node_data.items()
                          if k not in ['config', 'schedule_config', 'webhook_config']}
            with open(node_dir / 'config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(base_config, f)
                else:
                    yaml.dump(base_config, f, default_flow_style=False, allow_unicode=True)

            # 保存节点元数据
            metadata = {
                'node_id': node_id,
                'node_type': node_type,
                'title': node_data.get('title', ''),
                'trigger_type': trigger_config['trigger_type'],
                'position': node.get('position', {}),
                'config_files': ['trigger_config.yaml', 'config.yaml']
            }

            with open(node_dir / 'metadata.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            # 记录位置映射
            self.node_positions[node_id] = {
                'node_dir': str(node_dir.relative_to(self.output_dir)),
                'node_type': node_type,
                'index_in_yaml': None
            }

            return True

        except Exception as e:
            self.logger.error(f"拆分触发器节点失败: {e}")
            return False

    def _split_generic_node(self, node: Dict, node_dir: Path) -> bool:
        """拆分通用节点"""
        try:
            node_data = node.get('data', {})
            node_id = node.get('id', 'unknown')
            node_type = node_data.get('type', 'unknown')

            # 保存完整配置
            with open(node_dir / 'config.yaml', 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(node_data, f)
                else:
                    yaml.dump(node_data, f, default_flow_style=False, allow_unicode=True)

            # 保存节点元数据
            metadata = {
                'node_id': node_id,
                'node_type': node_type,
                'title': node_data.get('title', ''),
                'position': node.get('position', {})
            }

            with open(node_dir / 'metadata.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            # 记录位置映射
            self.node_positions[node_id] = {
                'node_dir': str(node_dir.relative_to(self.output_dir)),
                'node_type': node_type,
                'index_in_yaml': None
            }

            return True

        except Exception as e:
            self.logger.error(f"拆分通用节点失败: {e}")
            return False
    
    def _save_metadata(self):
        """保存元数据和映射信息"""
        try:
            # 保存版本信息
            with open(self.output_dir / 'metadata' / 'version_info.json', 'w', encoding='utf-8') as f:
                json.dump(self.metadata, f, ensure_ascii=False, indent=2)
            
            # 保存节点位置映射
            with open(self.output_dir / 'metadata' / 'node_positions.json', 'w', encoding='utf-8') as f:
                json.dump(self.node_positions, f, ensure_ascii=False, indent=2)
            
            # 保存原始结构（用于重建）
            structure_info = {
                'original_file': str(self.yaml_file),
                'total_lines': len(self.raw_yaml_lines),
                'workflow_structure': {
                    'app': self.workflow_data.get('app', {}),
                    'dependencies': self.workflow_data.get('dependencies', []),
                    'kind': self.workflow_data.get('kind', ''),
                    'version': self.workflow_data.get('version', ''),
                    'workflow_meta': {
                        'conversation_variables': self.workflow_data.get('workflow', {}).get('conversation_variables', []),
                        'environment_variables': self.workflow_data.get('workflow', {}).get('environment_variables', []),
                        'features': self.workflow_data.get('workflow', {}).get('features', {}),
                        'edges': self.workflow_data.get('workflow', {}).get('graph', {}).get('edges', [])
                    }
                }
            }
            
            with open(self.output_dir / 'metadata' / 'original_structure.json', 'w', encoding='utf-8') as f:
                json.dump(structure_info, f, ensure_ascii=False, indent=2)
            
            # 创建变更日志
            change_log = {
                'created': datetime.now().isoformat(),
                'operations': [
                    {
                        'type': 'split',
                        'timestamp': datetime.now().isoformat(),
                        'source_file': str(self.yaml_file),
                        'output_dir': str(self.output_dir),
                        'node_count': self.metadata['statistics']['total_nodes']
                    }
                ]
            }
            
            with open(self.output_dir / 'metadata' / 'change_log.json', 'w', encoding='utf-8') as f:
                json.dump(change_log, f, ensure_ascii=False, indent=2)

            # 生成工具分析报告
            tools_report_path = self.output_dir / 'metadata' / 'tools_analysis_report.md'
            self.tool_manager.generate_tool_report(self.metadata['tools_analysis'], str(tools_report_path))
            
        except Exception as e:
            self.logger.error(f"保存元数据失败: {e}")


class WorkflowRebuilder:
    """工作流重建器 - 将拆分的文件重新组合成YAML"""
    
    def __init__(self, split_dir: str, enable_parallel: bool = True):
        self.split_dir = Path(split_dir)
        self.logger = logging.getLogger(__name__)

        # 并行处理配置
        self.enable_parallel = enable_parallel
        self.parallel_processor = WorkflowParallelProcessor() if enable_parallel else None

        # 错误处理
        self.error_handler = ErrorHandler()

        # 初始化YAML处理器
        if YAML_AVAILABLE:
            self.yaml_processor = YAML()
            self.yaml_processor.preserve_quotes = True
            self.yaml_processor.width = 4096
        else:
            self.yaml_processor = None
    
    def rebuild_workflow(self, output_file: str = None, validate_dsl: bool = True) -> bool:
        """重建工作流

        Args:
            output_file: 输出文件名
            validate_dsl: 是否验证重建后的DSL结构
        """
        def _rebuild_operation(output_file_param):
            # 确定输出文件名
            final_output_file = output_file_param
            if final_output_file is None:
                dir_info = WorkflowVersionManager.parse_dirname_info(self.split_dir.name)
                version = dir_info.get('version', 'V1.0')
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                final_output_file = f"rebuilt_workflow_{version}_{timestamp}.yml"

            self.logger.info(f"开始重建工作流: {self.split_dir} -> {final_output_file}")

            # 加载元数据
            metadata = self._load_metadata()
            if not metadata:
                raise Exception("元数据加载失败")

            # 重建YAML结构
            rebuilt_data = self._rebuild_yaml_structure(metadata)
            if not rebuilt_data:
                raise Exception("YAML结构重建失败")

            # 验证重建后的DSL结构
            if validate_dsl:
                self.logger.info("验证重建后的DSL结构...")
                is_valid, validation_errors = DSLValidator.validate_dsl_structure(rebuilt_data)
                if not is_valid:
                    error_msg = f"重建后的DSL结构验证失败: {', '.join(validation_errors)}"
                    self.logger.error(error_msg)
                    raise Exception(error_msg)
                self.logger.info("重建后的DSL结构验证通过")

            # 写入文件
            output_path = Path(final_output_file)
            with open(output_path, 'w', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    self.yaml_processor.dump(rebuilt_data, f)
                else:
                    yaml.dump(rebuilt_data, f, default_flow_style=False, allow_unicode=True)

            self.logger.info(f"工作流重建完成: {output_path}")
            return True

        try:
            return self.error_handler.handle_operation(lambda: _rebuild_operation(output_file), "工作流重建")
        except Exception as e:
            self.logger.error(f"工作流重建失败: {e}")
            # 生成错误报告
            error_report = self.error_handler.generate_error_report(
                self.split_dir / "logs" / "rebuild_error_report.md" if self.split_dir.exists() else None
            )
            if error_report != "无错误记录":
                self.logger.info("重建错误报告已生成")
            return False
    
    def _load_metadata(self) -> Optional[Dict]:
        """加载元数据"""
        try:
            metadata_files = {
                'version_info': self.split_dir / 'metadata' / 'version_info.json',
                'node_positions': self.split_dir / 'metadata' / 'node_positions.json', 
                'original_structure': self.split_dir / 'metadata' / 'original_structure.json'
            }
            
            metadata = {}
            for key, file_path in metadata_files.items():
                if file_path.exists():
                    with open(file_path, 'r', encoding='utf-8') as f:
                        metadata[key] = json.load(f)
                else:
                    self.logger.error(f"元数据文件不存在: {file_path}")
                    return None
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"加载元数据失败: {e}")
            return None
    
    def _rebuild_yaml_structure(self, metadata: Dict) -> Optional[Dict]:
        """重建YAML结构"""
        try:
            # 获取原始结构
            original_structure = metadata['original_structure']['workflow_structure']
            
            # 重建基础结构
            rebuilt_data = {
                'app': original_structure['app'],
                'dependencies': original_structure['dependencies'],
                'kind': original_structure['kind'],
                'version': original_structure['version'],
                'workflow': {
                    'conversation_variables': original_structure['workflow_meta']['conversation_variables'],
                    'environment_variables': original_structure['workflow_meta']['environment_variables'],
                    'features': original_structure['workflow_meta']['features'],
                    'graph': {
                        'edges': original_structure['workflow_meta']['edges'],
                        'nodes': []
                    }
                }
            }
            
            # 重建节点
            nodes = self._rebuild_nodes(metadata['node_positions'])
            if nodes is None:
                return None
            
            rebuilt_data['workflow']['graph']['nodes'] = nodes
            
            return rebuilt_data
            
        except Exception as e:
            self.logger.error(f"重建YAML结构失败: {e}")
            return None
    
    def _rebuild_nodes(self, node_positions: Dict) -> Optional[List]:
        """重建节点列表 - 支持并行处理"""
        try:
            nodes = []

            # 按节点类型和索引排序
            sorted_nodes = sorted(node_positions.items(),
                                key=lambda x: (x[1]['node_type'], x[1]['node_dir']))

            if self.enable_parallel and len(sorted_nodes) > 3:  # 只有在节点较多时才使用并行处理
                self.logger.info("使用并行处理重建节点...")

                # 准备节点重建任务
                def rebuild_node_task(node_info):
                    node_id, position_info = node_info
                    node_dir = self.split_dir / position_info['node_dir']
                    return self._rebuild_single_node(node_id, node_dir, position_info['node_type'])

                # 并行重建节点
                results = self.parallel_processor.process_nodes_parallel(
                    [{'node_id': node_id, 'position_info': position_info}
                     for node_id, position_info in sorted_nodes],
                    lambda task, **kwargs: rebuild_node_task((task['node_id'], task['position_info']))
                )

                # 收集结果（保持原始顺序）
                for node_id, position_info in sorted_nodes:
                    node = results.get(node_id)
                    if node:
                        nodes.append(node)
                    else:
                        self.logger.warning(f"重建节点失败: {node_id}")

            else:
                # 串行重建
                self.logger.info("使用串行处理重建节点...")
                for node_id, position_info in sorted_nodes:
                    node_dir = self.split_dir / position_info['node_dir']

                    # 重建单个节点
                    node = self._rebuild_single_node(node_id, node_dir, position_info['node_type'])
                    if node:
                        nodes.append(node)
                    else:
                        self.logger.warning(f"重建节点失败: {node_id}")

            return nodes

        except Exception as e:
            self.logger.error(f"重建节点列表失败: {e}")
            return None
    
    def _rebuild_single_node(self, node_id: str, node_dir: Path, node_type: str) -> Optional[Dict]:
        """重建单个节点"""
        try:
            # 加载节点元数据
            with open(node_dir / 'metadata.json', 'r', encoding='utf-8') as f:
                node_metadata = json.load(f)
            
            # 加载基础配置
            with open(node_dir / 'config.yaml', 'r', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    node_config = self.yaml_processor.load(f)
                else:
                    node_config = yaml.safe_load(f)
            
            # 根据节点类型重建特定内容 - 支持更多Dify节点类型
            if node_type == 'llm':
                return self._rebuild_llm_node(node_id, node_dir, node_metadata, node_config)
            elif node_type == 'code':
                return self._rebuild_code_node(node_id, node_dir, node_metadata, node_config)
            elif node_type == 'agent':
                return self._rebuild_agent_node(node_id, node_dir, node_metadata, node_config)
            elif node_type == 'tool':
                return self._rebuild_tool_node(node_id, node_dir, node_metadata, node_config)
            elif node_type == 'http_request':
                return self._rebuild_http_request_node(node_id, node_dir, node_metadata, node_config)
            elif node_type == 'if_else':
                return self._rebuild_if_else_node(node_id, node_dir, node_metadata, node_config)
            elif node_type == 'iteration':
                return self._rebuild_iteration_node(node_id, node_dir, node_metadata, node_config)
            elif node_type == 'template_transform':
                return self._rebuild_template_transform_node(node_id, node_dir, node_metadata, node_config)
            elif node_type in ['start', 'end']:
                return self._rebuild_flow_control_node(node_id, node_metadata, node_config)
            elif node_type.startswith('trigger_'):
                return self._rebuild_trigger_node(node_id, node_dir, node_metadata, node_config)
            else:
                return self._rebuild_generic_node(node_id, node_metadata, node_config)
                
        except Exception as e:
            self.logger.error(f"重建单个节点失败 {node_id}: {e}")
            return None
    
    def _rebuild_llm_node(self, node_id: str, node_dir: Path, metadata: Dict, config: Dict) -> Dict:
        """重建LLM节点"""
        # 重建提示词模板
        prompt_template = []
        
        for prompt_file in metadata.get('prompt_files', []):
            prompt_path = node_dir / prompt_file
            if prompt_path.exists():
                with open(prompt_path, 'r', encoding='utf-8') as f:
                    prompt_text = f.read()
                
                # 从文件名提取角色信息
                role = 'user'
                if 'system_prompt' in prompt_file:
                    role = 'system'
                elif 'user_prompt' in prompt_file:
                    role = 'user'
                
                prompt_template.append({
                    'edition_type': 'basic',
                    'id': f"{node_id}_prompt_{len(prompt_template)}",
                    'role': role,
                    'text': prompt_text
                })
        
        # 合并配置
        config['prompt_template'] = prompt_template
        
        # 构建完整节点
        return {
            'data': config,
            'id': node_id,
            'position': metadata.get('position', {}),
            'height': metadata.get('height', 115),
            'width': metadata.get('width', 242),
            'selected': False,
            'sourcePosition': 'right',
            'targetPosition': 'left',
            'type': 'custom'
        }
    
    def _rebuild_code_node(self, node_id: str, node_dir: Path, metadata: Dict, config: Dict) -> Dict:
        """重建代码节点"""
        # 重建代码内容
        code_file = metadata.get('code_file', 'code.py')
        code_path = node_dir / code_file
        
        if code_path.exists():
            with open(code_path, 'r', encoding='utf-8') as f:
                code_content = f.read()
            config['code'] = code_content
        
        # 构建完整节点
        return {
            'data': config,
            'id': node_id,
            'position': metadata.get('position', {}),
            'height': metadata.get('height', 52),
            'width': metadata.get('width', 242),
            'selected': False,
            'sourcePosition': 'right',
            'targetPosition': 'left',
            'type': 'custom'
        }
    
    def _rebuild_agent_node(self, node_id: str, node_dir: Path, metadata: Dict, config: Dict) -> Dict:
        """重建Agent节点"""
        # 重建提示词模板
        prompt_template = []

        for prompt_file in metadata.get('prompt_files', []):
            prompt_path = node_dir / prompt_file
            if prompt_path.exists():
                with open(prompt_path, 'r', encoding='utf-8') as f:
                    prompt_text = f.read()

                # 从文件名提取角色信息
                role = 'user'
                if 'system_prompt' in prompt_file:
                    role = 'system'
                elif 'user_prompt' in prompt_file:
                    role = 'user'

                prompt_template.append({
                    'edition_type': 'basic',
                    'id': f"{node_id}_prompt_{len(prompt_template)}",
                    'role': role,
                    'text': prompt_text
                })

        # 加载Agent参数
        agent_parameters = {}
        agent_params_file = node_dir / 'agent_parameters.yaml'
        if agent_params_file.exists():
            with open(agent_params_file, 'r', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    agent_parameters = self.yaml_processor.load(f)
                else:
                    agent_parameters = yaml.safe_load(f)

        # 合并配置
        config['prompt_template'] = prompt_template
        config['agent_parameters'] = agent_parameters

        # 构建完整节点
        return {
            'data': config,
            'id': node_id,
            'position': metadata.get('position', {}),
            'height': metadata.get('height', 125),
            'width': metadata.get('width', 242),
            'selected': False,
            'sourcePosition': 'right',
            'targetPosition': 'left',
            'type': 'custom'
        }

    def _rebuild_tool_node(self, node_id: str, node_dir: Path, metadata: Dict, config: Dict) -> Dict:
        """重建工具节点"""
        # 加载工具配置
        tool_config = {}
        tool_config_file = node_dir / 'tool_config.yaml'
        if tool_config_file.exists():
            with open(tool_config_file, 'r', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    tool_config = self.yaml_processor.load(f)
                else:
                    tool_config = yaml.safe_load(f)

        # 合并配置
        config.update(tool_config)

        # 构建完整节点
        return {
            'data': config,
            'id': node_id,
            'position': metadata.get('position', {}),
            'height': metadata.get('height', 115),
            'width': metadata.get('width', 242),
            'selected': False,
            'sourcePosition': 'right',
            'targetPosition': 'left',
            'type': 'custom'
        }

    def _rebuild_http_request_node(self, node_id: str, node_dir: Path, metadata: Dict, config: Dict) -> Dict:
        """重建HTTP请求节点"""
        # 加载HTTP配置
        http_config = {}
        http_config_file = node_dir / 'http_config.yaml'
        if http_config_file.exists():
            with open(http_config_file, 'r', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    http_config = self.yaml_processor.load(f)
                else:
                    http_config = yaml.safe_load(f)

        # 合并配置
        config.update(http_config)

        # 构建完整节点
        return {
            'data': config,
            'id': node_id,
            'position': metadata.get('position', {}),
            'height': metadata.get('height', 115),
            'width': metadata.get('width', 242),
            'selected': False,
            'sourcePosition': 'right',
            'targetPosition': 'left',
            'type': 'custom'
        }

    def _rebuild_if_else_node(self, node_id: str, node_dir: Path, metadata: Dict, config: Dict) -> Dict:
        """重建条件分支节点"""
        # 加载逻辑配置
        logic_config = {}
        logic_config_file = node_dir / 'logic_config.yaml'
        if logic_config_file.exists():
            with open(logic_config_file, 'r', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    logic_config = self.yaml_processor.load(f)
                else:
                    logic_config = yaml.safe_load(f)

        # 合并配置
        config.update(logic_config)

        # 构建完整节点
        return {
            'data': config,
            'id': node_id,
            'position': metadata.get('position', {}),
            'height': metadata.get('height', 95),
            'width': metadata.get('width', 242),
            'selected': False,
            'sourcePosition': 'right',
            'targetPosition': 'left',
            'type': 'custom'
        }

    def _rebuild_iteration_node(self, node_id: str, node_dir: Path, metadata: Dict, config: Dict) -> Dict:
        """重建迭代节点"""
        # 加载迭代配置
        iteration_config = {}
        iteration_config_file = node_dir / 'iteration_config.yaml'
        if iteration_config_file.exists():
            with open(iteration_config_file, 'r', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    iteration_config = self.yaml_processor.load(f)
                else:
                    iteration_config = yaml.safe_load(f)

        # 合并配置
        config.update(iteration_config)

        # 构建完整节点
        return {
            'data': config,
            'id': node_id,
            'position': metadata.get('position', {}),
            'height': metadata.get('height', 95),
            'width': metadata.get('width', 242),
            'selected': False,
            'sourcePosition': 'right',
            'targetPosition': 'left',
            'type': 'custom'
        }

    def _rebuild_template_transform_node(self, node_id: str, node_dir: Path, metadata: Dict, config: Dict) -> Dict:
        """重建模板转换节点"""
        # 加载模板内容
        template_file = node_dir / 'template.txt'
        if template_file.exists():
            with open(template_file, 'r', encoding='utf-8') as f:
                config['template'] = f.read()

        # 加载变量选择器配置
        variable_selectors_file = node_dir / 'variable_selectors.yaml'
        if variable_selectors_file.exists():
            with open(variable_selectors_file, 'r', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    config['variable_selectors'] = self.yaml_processor.load(f)
                else:
                    config['variable_selectors'] = yaml.safe_load(f)

        # 构建完整节点
        return {
            'data': config,
            'id': node_id,
            'position': metadata.get('position', {}),
            'height': metadata.get('height', 95),
            'width': metadata.get('width', 242),
            'selected': False,
            'sourcePosition': 'right',
            'targetPosition': 'left',
            'type': 'custom'
        }

    def _rebuild_flow_control_node(self, node_id: str, metadata: Dict, config: Dict) -> Dict:
        """重建流程控制节点"""
        # 构建完整节点
        return {
            'data': config,
            'id': node_id,
            'position': metadata.get('position', {}),
            'height': metadata.get('height', 54),
            'width': metadata.get('width', 120),
            'selected': False,
            'sourcePosition': 'right',
            'targetPosition': 'left',
            'type': 'custom'
        }

    def _rebuild_trigger_node(self, node_id: str, node_dir: Path, metadata: Dict, config: Dict) -> Dict:
        """重建触发器节点"""
        # 加载触发器配置
        trigger_config = {}
        trigger_config_file = node_dir / 'trigger_config.yaml'
        if trigger_config_file.exists():
            with open(trigger_config_file, 'r', encoding='utf-8') as f:
                if YAML_AVAILABLE:
                    trigger_config = self.yaml_processor.load(f)
                else:
                    trigger_config = yaml.safe_load(f)

        # 合并配置
        config.update(trigger_config)

        # 构建完整节点
        return {
            'data': config,
            'id': node_id,
            'position': metadata.get('position', {}),
            'height': metadata.get('height', 54),
            'width': metadata.get('width', 120),
            'selected': False,
            'sourcePosition': 'right',
            'targetPosition': 'left',
            'type': 'custom'
        }

    def _rebuild_generic_node(self, node_id: str, metadata: Dict, config: Dict) -> Dict:
        """重建通用节点"""
        return {
            'data': config,
            'id': node_id,
            'position': metadata.get('position', {}),
            'height': metadata.get('height', 115),
            'width': metadata.get('width', 242),
            'selected': False,
            'sourcePosition': 'right',
            'targetPosition': 'left',
            'type': 'custom'
        }


class WorkflowComparator:
    """工作流对比器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def compare_workflows(self, file1: str, file2: str, output_file: str = None) -> bool:
        """对比两个工作流文件"""
        try:
            self.logger.info(f"开始对比工作流: {file1} vs {file2}")
            
            # 读取文件内容
            with open(file1, 'r', encoding='utf-8') as f:
                content1 = f.readlines()
            
            with open(file2, 'r', encoding='utf-8') as f:
                content2 = f.readlines()
            
            # 生成差异
            differ = difflib.unified_diff(
                content1, content2,
                fromfile=file1,
                tofile=file2,
                lineterm='',
                n=3
            )
            
            # 确定输出文件
            if output_file is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"workflow_diff_{timestamp}.html"
            
            # 生成HTML报告
            self._generate_html_diff_report(file1, file2, content1, content2, output_file)
            
            # 生成文本差异
            text_diff_file = output_file.replace('.html', '.txt')
            with open(text_diff_file, 'w', encoding='utf-8') as f:
                f.write(f"工作流对比报告\n")
                f.write(f"文件1: {file1}\n")
                f.write(f"文件2: {file2}\n")
                f.write(f"生成时间: {datetime.now()}\n")
                f.write("=" * 80 + "\n\n")
                
                differ = difflib.unified_diff(
                    content1, content2,
                    fromfile=file1,
                    tofile=file2,
                    lineterm='',
                    n=3
                )
                
                for line in differ:
                    f.write(line + '\n')
            
            self.logger.info(f"对比报告生成完成: {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"对比工作流失败: {e}")
            return False
    
    def _generate_html_diff_report(self, file1: str, file2: str, content1: List[str], 
                                 content2: List[str], output_file: str):
        """生成HTML差异报告"""
        # 生成HTML差异视图
        differ = difflib.HtmlDiff()
        html_diff = differ.make_file(
            content1, content2,
            fromdesc=f"原始文件: {file1}",
            todesc=f"重建文件: {file2}",
            context=True,
            numlines=3
        )
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_diff)


def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("Dify Workflow Manager v2.0 - 增强版")
        print("=" * 50)
        print("支持Dify 40+种节点类型，DSL验证，并行处理")
        print()
        print("使用方法:")
        print("  python dify_workflow_manager_v2.py split <yaml_file> [--no-parallel] [--no-validate]")
        print("  python dify_workflow_manager_v2.py rebuild <split_dir> [--no-parallel] [--no-validate]")
        print("  python dify_workflow_manager_v2.py compare <file1> <file2>")
        print("  python dify_workflow_manager_v2.py validate <yaml_file>")
        print("  python dify_workflow_manager_v2.py analyze-tools <yaml_file>")
        print("  python dify_workflow_manager_v2.py check-version <yaml_file>")
        print("  python dify_workflow_manager_v2.py migrate <yaml_file> <target_version> [output_file]")
        print()
        print("选项:")
        print("  --no-parallel    禁用并行处理")
        print("  --no-validate    禁用DSL验证")
        print()
        print("示例:")
        print("  python dify_workflow_manager_v2.py split \"AI Code Review-V4.1.yml\"")
        print("  python dify_workflow_manager_v2.py rebuild \"parsed_workflow_V4.1_20240115_143022\" --no-parallel")
        print("  python dify_workflow_manager_v2.py validate \"workflow.yml\"")
        return 1
    
    command = sys.argv[1].lower()

    # 解析选项
    enable_parallel = True
    validate_dsl = True

    args = sys.argv[2:]
    filtered_args = []

    for arg in args:
        if arg == '--no-parallel':
            enable_parallel = False
        elif arg == '--no-validate':
            validate_dsl = False
        else:
            filtered_args.append(arg)

    if command == 'split':
        if len(filtered_args) < 1:
            print("错误: 请提供YAML文件路径")
            return 1

        yaml_file = filtered_args[0]
        output_dir = filtered_args[1] if len(filtered_args) > 1 else None

        splitter = WorkflowSplitter(yaml_file, output_dir, enable_parallel=enable_parallel)
        success = splitter.split_workflow(validate_dsl=validate_dsl)

        if success:
            parallel_info = " (并行处理)" if enable_parallel else " (串行处理)"
            print(f"[SUCCESS] 工作流拆分成功{parallel_info}!")
            print(f"[OUTPUT] 输出目录: {splitter.output_dir}")
        else:
            print("[ERROR] 工作流拆分失败!")
            return 1
    
    elif command == 'rebuild':
        if len(filtered_args) < 1:
            print("错误: 请提供拆分目录路径")
            return 1

        split_dir = filtered_args[0]
        output_file = filtered_args[1] if len(filtered_args) > 1 else None

        rebuilder = WorkflowRebuilder(split_dir, enable_parallel=enable_parallel)
        success = rebuilder.rebuild_workflow(output_file, validate_dsl=validate_dsl)

        if success:
            parallel_info = " (并行处理)" if enable_parallel else " (串行处理)"
            print(f"[SUCCESS] 工作流重建成功{parallel_info}!")
        else:
            print("[ERROR] 工作流重建失败!")
            return 1
    
    elif command == 'validate':
        if len(filtered_args) < 1:
            print("错误: 请提供YAML文件路径")
            return 1

        yaml_file = filtered_args[0]

        # 加载并验证DSL
        try:
            if YAML_AVAILABLE:
                yaml_processor = YAML()
                with open(yaml_file, 'r', encoding='utf-8') as f:
                    workflow_data = yaml_processor.load(f)
            else:
                with open(yaml_file, 'r', encoding='utf-8') as f:
                    workflow_data = yaml.safe_load(f)

            print(f"正在验证DSL文件: {yaml_file}")
            is_valid, validation_errors = DSLValidator.validate_dsl_structure(workflow_data)

            if is_valid:
                print("[SUCCESS] DSL结构验证通过!")
                print("工作流结构完整且符合Dify规范")
            else:
                print("[ERROR] DSL结构验证失败:")
                for error in validation_errors:
                    print(f"  - {error}")
                return 1

        except Exception as e:
            print(f"[ERROR] DSL验证过程中发生错误: {e}")
            return 1

    elif command == 'analyze-tools':
        if len(filtered_args) < 1:
            print("错误: 请提供YAML文件路径")
            return 1

        yaml_file = filtered_args[0]
        output_file = filtered_args[1] if len(filtered_args) > 1 else None

        # 加载并分析工具
        try:
            if YAML_AVAILABLE:
                yaml_processor = YAML()
                with open(yaml_file, 'r', encoding='utf-8') as f:
                    workflow_data = yaml_processor.load(f)
            else:
                with open(yaml_file, 'r', encoding='utf-8') as f:
                    workflow_data = yaml.safe_load(f)

            print(f"正在分析工作流工具: {yaml_file}")

            tool_manager = ToolManager()
            tools_analysis = tool_manager.analyze_tools_in_workflow(workflow_data)

            # 生成报告
            if output_file:
                report_path = output_file
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                report_path = f"tools_analysis_{timestamp}.md"

            report = tool_manager.generate_tool_report(tools_analysis, report_path)
            print("[SUCCESS] 工具分析完成!")
            print(f"[REPORT] 报告已保存到: {report_path}")

            # 在控制台显示关键统计信息
            stats = tools_analysis['statistics']
            print(f"[STATS] 工具统计: 总计 {stats['total_tools']} 个工具，{stats['enabled_tools']} 个启用")

        except Exception as e:
            print(f"[ERROR] 工具分析过程中发生错误: {e}")
            return 1

    elif command == 'check-version':
        if len(filtered_args) < 1:
            print("错误: 请提供YAML文件路径")
            return 1

        yaml_file = filtered_args[0]

        try:
            if YAML_AVAILABLE:
                yaml_processor = YAML()
                with open(yaml_file, 'r', encoding='utf-8') as f:
                    workflow_data = yaml_processor.load(f)
            else:
                with open(yaml_file, 'r', encoding='utf-8') as f:
                    workflow_data = yaml.safe_load(f)

            print(f"正在检查工作流版本: {yaml_file}")

            # 生成版本报告
            version_report = WorkflowVersionManager.generate_version_report(workflow_data)

            # 显示报告
            print(version_report)

        except Exception as e:
            print(f"[ERROR] 版本检查过程中发生错误: {e}")
            return 1

    elif command == 'migrate':
        if len(filtered_args) < 2:
            print("错误: 请提供YAML文件路径和目标版本")
            return 1

        yaml_file = filtered_args[0]
        target_version = filtered_args[1]
        output_file = filtered_args[2] if len(filtered_args) > 2 else None

        try:
            if YAML_AVAILABLE:
                yaml_processor = YAML()
                with open(yaml_file, 'r', encoding='utf-8') as f:
                    workflow_data = yaml_processor.load(f)
            else:
                with open(yaml_file, 'r', encoding='utf-8') as f:
                    workflow_data = yaml.safe_load(f)

            print(f"正在迁移工作流版本: {yaml_file}")
            print(f"目标版本: {target_version}")

            # 执行迁移
            success, migrated_data, migration_log = WorkflowVersionManager.migrate_workflow(
                workflow_data, target_version
            )

            if success:
                print("[SUCCESS] 版本迁移成功!")

                # 显示迁移日志
                for log_entry in migration_log:
                    print(f"  - {log_entry}")

                # 保存迁移后的文件
                if output_file:
                    output_path = output_file
                else:
                    base_name = Path(yaml_file).stem
                    output_path = f"{base_name}_migrated_{target_version}.yml"

                with open(output_path, 'w', encoding='utf-8') as f:
                    if YAML_AVAILABLE:
                        yaml_processor.dump(migrated_data, f)
                    else:
                        yaml.dump(migrated_data, f, default_flow_style=False, allow_unicode=True)

                print(f"[OUTPUT] 迁移后的文件已保存到: {output_path}")

            else:
                print("[ERROR] 版本迁移失败:")
                for error in migration_log:
                    print(f"  - {error}")
                return 1

        except Exception as e:
            print(f"[ERROR] 版本迁移过程中发生错误: {e}")
            return 1

    elif command == 'compare':
        if len(filtered_args) < 2:
            print("错误: 请提供两个文件路径")
            return 1

        file1 = filtered_args[0]
        file2 = filtered_args[1]
        output_file = filtered_args[2] if len(filtered_args) > 2 else None

        comparator = WorkflowComparator()
        success = comparator.compare_workflows(file1, file2, output_file)

        if success:
            print("[SUCCESS] 工作流对比完成!")
        else:
            print("[ERROR] 工作流对比失败!")
            return 1
    
    else:
        print(f"错误: 未知命令 '{command}'")
        print("支持的命令: split, rebuild, compare")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())






