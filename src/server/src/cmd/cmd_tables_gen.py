#!/usr/bin/env python3

import os
import glob
import json

ARG_TYPES = {
    "string": "String",
    "integer": "Integer",
    "double": "Double",
    "key": "Key",
    "pattern": "Pattern",
    "unix-time": "UnixTime",
    "pure-token": "UnixTime",
    "oneof": "OneOf",
    "block": "Block",
}

GROUPS = {
    "generic": "Generic",
    "string": "String",
    "list": "List",
    "set": "Set",
    "sorted_set": "SortedSet",
    "hash": "Hash",
    "pubsub": "Pubsub",
    "transactions": "Transactions",
    "connection": "Connection",
    "server": "Server",
    "scripting": "Scripting",
    "hyperloglog": "Hyperloglog",
    "cluster": "Cluster",
    "sentinel": "Sentinel",
    "geo": "Geo",
    "stream": "Stream",
    "bitmap": "Bitmap",
}

RESP2_TYPES = {
    "simple-string": "RESP2_SIMPLE_STRING",
    "error": "RESP2_ERROR",
    "integer": "RESP2_INTEGER",
    "bulk-string": "RESP2_BULK_STRING",
    "null-bulk-string": "RESP2_NULL_BULK_STRING",
    "array": "RESP2_ARRAY",
    "null-array": "RESP2_NULL_ARRAY",
}

RESP3_TYPES = {
    "simple-string": "RESP3_SIMPLE_STRING",
    "error": "RESP3_ERROR",
    "integer": "RESP3_INTEGER",
    "double": "RESP3_DOUBLE",
    "bulk-string": "RESP3_BULK_STRING",
    "array": "RESP3_ARRAY",
    "map": "RESP3_MAP",
    "set": "RESP3_SET",
    "bool": "RESP3_BOOL",
    "null": "RESP3_NULL",
}


def get_optional_desc_string(desc, field, force_uppercase=False):
    v = desc.get(field, None)
    if v and force_uppercase:
        v = v.upper()
    ret = "\"%s\"" % v if v else "\"\""
    return ret.replace("\n", "\\n")


# Globals
# container_name -> dict(subcommand_name -> Subcommand) - Only subcommands
subcommands = {}
commands = {}  # command_name -> Command - Only commands


class KeySpec(object):
    def __init__(self, spec):
        self.spec = spec

    def struct_code(self):
        def _flags_code():
            s = ""
            for flag in self.spec.get("flags", []):
                s += "CMD_KEY_%s|" % flag
            return s[:-1] if s else 0

        def _begin_search_code():
            if self.spec["begin_search"].get("index"):
                return "BeginSearch::Index(%d)" % (
                    self.spec["begin_search"]["index"]["pos"]
                )
            elif self.spec["begin_search"].get("keyword"):
                return "BeginSearch::Keyword { keyword: \"%s\".to_string(), start_from: %d }" % (
                    self.spec["begin_search"]["keyword"]["keyword"],
                    self.spec["begin_search"]["keyword"]["startfrom"],
                )
            elif "unknown" in self.spec["begin_search"]:
                return "BeginSearch::Unknown"
            else:
                print("Invalid begin_search! value=%s" %
                      self.spec["begin_search"])
                exit(1)

        def _find_keys_code():
            if self.spec["find_keys"].get("range"):
                return "FindKeys::Range { last_key: %d, key_step: %d, limit: %d }" % (
                    self.spec["find_keys"]["range"]["lastkey"],
                    self.spec["find_keys"]["range"]["step"],
                    self.spec["find_keys"]["range"]["limit"]
                )
            elif self.spec["find_keys"].get("keynum"):
                return "FindKeys::KeyNum { key_num_index: %d, first_key_index: %d, key_step: %d }" % (
                    self.spec["find_keys"]["keynum"]["keynumidx"],
                    self.spec["find_keys"]["keynum"]["firstkey"],
                    self.spec["find_keys"]["keynum"]["step"]
                )
            elif "unknown" in self.spec["find_keys"]:
                return "FindKeys::Unknown"
            else:
                print("Invalid find_keys! value=%s" % self.spec["find_keys"])
                exit(1)

        return "notes: %s.to_string(), flags: %s, bs: %s, fk: %s" % (
            get_optional_desc_string(self.spec, "notes"),
            _flags_code(),
            _begin_search_code(),
            _find_keys_code()
        )


class Argument(object):
    def __init__(self, parent_name, desc):
        self.desc = desc
        self.name = self.desc["name"].lower()
        self.type = self.desc["type"]
        self.parent_name = parent_name
        self.subargs = []
        self.subargs_name = None
        if self.type in ["oneof", "block"]:
            for subdesc in self.desc["arguments"]:
                self.subargs.append(Argument(self.fullname(), subdesc))

    def fullname(self):
        return ("%s %s" % (self.parent_name, self.name)).replace("-", "_")

    def struct_name(self):
        return ("%s_arg" % (self.fullname().replace(" ", "_"))).lower()

    def subarg_table_name(self):
        assert self.subargs
        return ("%s_aubargs" % (self.fullname().replace(" ", "_"))).lower()

    def struct_code(self):
        """
        Output example:
        "expiration",ARG_TYPE_ONEOF,NULL,NULL,NULL,CMD_ARG_OPTIONAL,.value.subargs=SET_expiration_Subargs
        """

        def _flags_code():
            s = ""
            if self.desc.get("optional", False):
                s += "CMD_ARG_OPTIONAL|"
            if self.desc.get("multiple", False):
                s += "CMD_ARG_MULTIPLE|"
            if self.desc.get("multiple_token", False):
                assert self.desc.get("multiple", False)  # Sanity
                s += "CMD_ARG_MULTIPLE_TOKEN|"
            return s[:-1] if s else "CMD_ARG_NONE"

        s = "name: \"%s\".to_string(), typ: ArgType::%s, key_spec_index: %d, token: %s.to_string(), summary: %s.to_string(), since: %s.to_string(), flag: %s" % (
            self.name,
            ARG_TYPES[self.type],
            self.desc.get("key_spec_index", -1),
            get_optional_desc_string(self.desc, "token", force_uppercase=True),
            get_optional_desc_string(self.desc, "summary"),
            get_optional_desc_string(self.desc, "since"),
            _flags_code(),
        )
        if "deprecated_since" in self.desc:
            s += ", deprecated_since: \"%s\".to_string()" % self.desc["deprecated_since"]
        else:
            s += ", deprecated_since:  \"\".to_string()"

        if self.subargs:
            s += ", sub_args: %s" % self.subarg_table_name()
        else:
            s += ", sub_args: vec![]"

        return s

    def write_internal_structs(self, f):
        if self.subargs:
            for subarg in self.subargs:
                subarg.write_internal_structs(f)

            f.write("// %s argument table\n" % self.fullname())
            f.write(
                "let %s = vec![\n" % self.subarg_table_name())
            for subarg in self.subargs:
                f.write("Arg{%s},\n" % subarg.struct_code())
            f.write("];\n\n")


class Command(object):
    def __init__(self, name, desc):
        self.name = name.upper()
        self.desc = desc
        self.group = self.desc["group"]
        self.subcommands = []
        self.args = []
        for arg_desc in self.desc.get("arguments", []):
            self.args.append(Argument(self.fullname(), arg_desc))

    def fullname(self):
        return self.name.replace("-", "_").replace(":", "")

    def return_types_table_name(self):
        return ("%s_return_info" % self.fullname().replace(" ", "_")).lower()

    def subcommand_table_name(self):
        assert self.subcommands
        return ("%s_subcommands" % self.name).lower()

    def history_table_name(self):
        return ("%s_history" % (self.fullname().replace(" ", "_"))).lower()

    def tips_table_name(self):
        return ("%s_tips" % (self.fullname().replace(" ", "_"))).lower()

    def arg_table_name(self):
        return ("%s_args" % (self.fullname().replace(" ", "_"))).lower()

    def struct_name(self):
        return ("%s_command" % (self.fullname().replace(" ", "_"))).lower()

    def history_code(self):
        if not self.desc.get("history"):
            return ""
        s = ""
        for tupl in self.desc["history"]:
            s += "{\"%s\",\"%s\"},\n" % (tupl[0], tupl[1])
        s += "{0}"
        return s

    def tips_code(self):
        if not self.desc.get("command_tips"):
            return ""
        s = ""
        for hint in self.desc["command_tips"]:
            s += "\"%s\".to_string()," % hint.lower()
        return s

    def struct_code(self):
        """
        Output example:
        "set","Set the string value of a key","O(1)","1.0.0",CMD_DOC_NONE,NULL,NULL,COMMAND_GROUP_STRING,SET_History,SET_tips,setCommand,-3,"write denyoom @string",{{"write read",KSPEC_BS_INDEX,.bs.index={1},KSPEC_FK_RANGE,.fk.range={0,1,0}}},.args=SET_Args
        """

        def _flags_code():
            s = ""
            for flag in self.desc.get("command_flags", []):
                s += "CMD_%s|" % flag
            return s[:-1] if s else 0

        def _acl_categories_code():
            s = ""
            for cat in self.desc.get("acl_categories", []):
                s += "ACL_CATEGORY_%s|" % cat
            return s[:-1] if s else 0

        def _doc_flags_code():
            s = ""
            for flag in self.desc.get("doc_flags", []):
                s += "CMD_DOC_%s|" % flag
            return s[:-1] if s else "CMD_DOC_NONE"

        def _key_specs_code():
            s = ""
            for spec in self.desc.get("key_specs", []):
                s += "KeySpec{%s}," % KeySpec(spec).struct_code()
            return s[:-1]

        s = "name: \"%s\".to_string(), summary: %s.to_string(), complexity: %s.to_string(), since: %s.to_string(), doc_flags: %s,replaced_by: %s.to_string(),deprecated_since: %s.to_string(), group: Group::%s, tips: %s, parse: %s, arity: %d, flags: %s, acl_categories: %s," % (
            self.name.lower(),
            get_optional_desc_string(self.desc, "summary"),
            get_optional_desc_string(self.desc, "complexity"),
            get_optional_desc_string(self.desc, "since"),
            _doc_flags_code(),
            get_optional_desc_string(self.desc, "replaced_by"),
            get_optional_desc_string(self.desc, "deprecated_since"),
            GROUPS[self.group],
            # self.history_table_name(),
            self.tips_table_name(),
            self.desc.get("function", "NULL"),
            self.desc["arity"],
            _flags_code(),
            _acl_categories_code()
        )

        specs = _key_specs_code()
        if specs:
            s += "key_specs: vec![%s]," % specs
        else:
            s += "key_specs: vec![],"

        # if self.desc.get("get_keys_function"):
        #     s += "%s," % self.desc["get_keys_function"]

        if self.subcommands:
            s += "sub_cmds: Some(%s)," % self.subcommand_table_name()
        else:
            s += "sub_cmds: None,"

        if self.args:
            s += "args: %s," % self.arg_table_name()
        else:
            s += "args: vec![],"

        return s[:-1]

    def write_internal_structs(self, f):
        if self.subcommands:
            subcommand_list = sorted(
                self.subcommands, key=lambda cmd: cmd.name)
            for subcommand in subcommand_list:
                subcommand.write_internal_structs(f)

            f.write("// %s command table \n" % self.fullname())
            f.write("let %s = vec![\n" %
                    self.subcommand_table_name())
            for subcommand in subcommand_list:
                f.write("CommandDesc {%s},\n" % subcommand.struct_code())
            f.write("].into_iter().map(|d| (d.name.to_owned(), d)).collect();\n\n")

        f.write("// ********** %s ********************\n\n" % self.fullname())

        # f.write("// %s history\n" % self.fullname())
        # code = self.history_code()
        # if code:
        #     f.write("commandHistory %s[] = {\n" % self.history_table_name())
        #     f.write("%s\n" % code)
        #     f.write("};\n\n")
        # else:
        #     f.write("#define %s NULL\n\n" % self.history_table_name())

        f.write("// %s tips \n" % self.fullname())
        code = self.tips_code()
        if code:
            f.write("let %s = vec![\n" % self.tips_table_name())
            f.write("%s\n" % code)
            f.write("];\n\n")
        else:
            f.write("let %s = vec![];\n\n" % self.tips_table_name())

        if self.args:
            for arg in self.args:
                arg.write_internal_structs(f)

            f.write("// %s argument table\n" % self.fullname())
            f.write(
                "let %s = vec![\n" % self.arg_table_name())
            for arg in self.args:
                f.write("Arg{%s},\n" % arg.struct_code())
            f.write("];\n\n")


class Subcommand(Command):
    def __init__(self, name, desc):
        self.container_name = desc["container"].upper()
        super(Subcommand, self).__init__(name, desc)

    def fullname(self):
        return "%s %s" % (self.container_name, self.name.replace("-", "_").replace(":", ""))


def create_command(name, desc):
    if desc.get("container"):
        cmd = Subcommand(name.upper(), desc)
        subcommands.setdefault(desc["container"].upper(), {})[name] = cmd
    else:
        cmd = Command(name.upper(), desc)
        commands[name.upper()] = cmd


# MAIN

# Figure out where the sources are
srcdir = os.path.abspath(os.path.dirname(
    os.path.abspath(__file__)))

# Create all command objects
print("Processing json files...")
for filename in glob.glob('%s/tmpl/*.json' % srcdir):
    with open(filename, "r") as f:
        try:
            d = json.load(f)
            for name, desc in d.items():
                create_command(name, desc)
        except json.decoder.JSONDecodeError as err:
            print("Error processing %s: %s" % (filename, err))
            exit(1)

# Link subcommands to containers
print("Linking container command to subcommands...")
for command in commands.values():
    assert command.group
    if command.name not in subcommands:
        continue
    for subcommand in subcommands[command.name].values():
        assert not subcommand.group or subcommand.group == command.group
        subcommand.group = command.group
        command.subcommands.append(subcommand)

print("Generating commands.rs...")
with open("%s/cmd_tables.rs" % srcdir, "w") as f:
    f.write("""// Copyright 2022 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.""")
    f.write("\n\n// Automatically generated by %s, do not edit. \n" %
            os.path.basename(__file__))
    f.write(
        """
/* We have fabulous commands from
 * the fantastic
 * Redis Command Table! */\n
"""
    )

    f.write("use std::{collections::HashMap, vec};\n"
            "use super::*;\n"
            "use crate::Parse;\n\n")

    f.write("fn unimplemented(_: &CommandDescs, _: &mut Parse) -> crate::Result<Box<dyn super::CommandAction>> {\n"
            "unimplemented!()\n"
            "}\n\n")

    f.write("pub fn all_cmd_tables() -> HashMap<String, CommandDesc> {\n")

    command_list = sorted(
        commands.values(), key=lambda cmd: (cmd.group, cmd.name))
    for command in command_list:
        command.write_internal_structs(f)

    f.write("// Main command table\n")
    f.write("[\n")
    curr_group = None
    for command in command_list:
        if curr_group != command.group:
            curr_group = command.group
            f.write("// %s\n" % curr_group)
        f.write("CommandDesc {%s},\n" % command.struct_code())
    # f.write("{0}\n")
    f.write("].into_iter().map(|e| (e.name.clone(), e)).collect()\n")
    f.write("}\n")

print("Format newfile..")
os.system("cargo fmt --all")
print("All done, exiting.")


