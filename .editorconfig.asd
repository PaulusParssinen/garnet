# Remove the line below if you want to inherit .editorconfig settings from higher directories
root = true

# C# files
[*.cs]

#### Core EditorConfig Options ####

# Indentation and spacing
indent_size = 4
indent_style = space
tab_width = 4

# New line preferences
end_of_line = native
insert_final_newline = false

#### .NET Coding Conventions ####

# Temporary code analysis supressions
# IDE0004: Remove Unnecessary Cast
dotnet_diagnostic.IDE0004.severity = suggestion
# IDE0010: Add missing cases
dotnet_diagnostic.IDE0010.severity = suggestion
# IDE0072: Add missing cases
dotnet_diagnostic.IDE0072.severity = suggestion
# IDE0240: Remove redundant nullable directive
dotnet_diagnostic.IDE0240.severity = suggestion
# CS1573: Parameter has no matching param tag in the XML comment (but other parameters do)
dotnet_diagnostic.CS1573.severity = suggestion
# CS1591: Missing XML comment for publicly visible type or member
dotnet_diagnostic.CS1591.severity = suggestion

# Temporary security analysis supressions
# CA5359: Do not disable certificate validation
dotnet_diagnostic.CA5359.severity = suggestion

# Miscellaneous style rules
dotnet_separate_import_directive_groups = false
file_header_template = Copyright (c) Microsoft Corporation.\nCopyright (c) Paulus Pärssinen.\nLicensed under the MIT license.
dotnet_sort_system_directives_first = true
dotnet_style_predefined_type_for_locals_parameters_members = true:suggestion
dotnet_style_predefined_type_for_member_access = true:suggestion

# avoid this. unless absolutely necessary
dotnet_style_qualification_for_field = false:suggestion
dotnet_style_qualification_for_property = false:suggestion
dotnet_style_qualification_for_method = false:suggestion
dotnet_style_qualification_for_event = false:suggestion

# name all constant fields using PascalCase
dotnet_naming_rule.constant_fields_should_be_pascal_case.severity = suggestion
dotnet_naming_rule.constant_fields_should_be_pascal_case.symbols  = constant_fields
dotnet_naming_rule.constant_fields_should_be_pascal_case.style = static_prefix_style
dotnet_naming_symbols.constant_fields.applicable_kinds   = field
dotnet_naming_symbols.constant_fields.required_modifiers = const
dotnet_naming_style.pascal_case_style.capitalization = pascal_case

# static fields should be PascalCase
dotnet_naming_rule.static_fields_should_have_prefix.severity = suggestion
dotnet_naming_rule.static_fields_should_have_prefix.symbols  = static_fields
dotnet_naming_rule.static_fields_should_have_prefix.style = static_prefix_style
dotnet_naming_symbols.static_fields.applicable_kinds   = field
dotnet_naming_symbols.static_fields.required_modifiers = static
dotnet_naming_style.static_prefix_style.capitalization = pascal_case 

# internal and private fields should be camelCase
dotnet_naming_rule.camel_case_for_private_internal_fields.severity = suggestion
dotnet_naming_rule.camel_case_for_private_internal_fields.symbols  = private_internal_fields
dotnet_naming_rule.camel_case_for_private_internal_fields.style = camel_case_style
dotnet_naming_symbols.private_internal_fields.applicable_kinds = field
dotnet_naming_symbols.private_internal_fields.applicable_accessibilities = private, internal
dotnet_naming_style.camel_case_style.capitalization = camel_case 

# Code quality
dotnet_style_readonly_field = true:suggestion
csharp_style_prefer_readonly_struct_member = true:suggestion
dotnet_code_quality_unused_parameters = non_public:suggestion

# Expression-level preferences
dotnet_style_object_initializer = true:suggestion
dotnet_style_collection_initializer = true:suggestion
dotnet_style_explicit_tuple_names = true:suggestion
dotnet_style_coalesce_expression = true:suggestion
dotnet_style_null_propagation = true:suggestion
dotnet_style_prefer_is_null_check_over_reference_equality_method = true:suggestion
dotnet_style_prefer_inferred_tuple_names = true:suggestion
dotnet_style_prefer_inferred_anonymous_type_member_names = true:suggestion
dotnet_style_prefer_auto_properties = true:suggestion
dotnet_style_prefer_conditional_expression_over_assignment = true:refactoring
dotnet_style_prefer_conditional_expression_over_return = true:refactoring
dotnet_style_require_accessibility_modifiers = omit_if_default:refactoring

# C# files
[*.cs]
# New line preferences
csharp_new_line_before_open_brace = all
csharp_new_line_before_else = true
csharp_new_line_before_catch = true
csharp_new_line_before_finally = true
csharp_new_line_before_members_in_object_initializers = true
csharp_new_line_before_members_in_anonymous_types = true
csharp_new_line_between_query_expression_clauses = true
place_attribute_on_same_line = false
csharp_place_field_attribute_on_same_line = false

# Indentation preferences
csharp_indent_block_contents = true
csharp_indent_braces = false
csharp_indent_case_contents = true
csharp_indent_case_contents_when_block = true
csharp_indent_switch_labels = true
csharp_indent_labels = one_less_than_current

# Modifier preferences
csharp_preferred_modifier_order = public,private,protected,internal,static,extern,new,virtual,abstract,sealed,override,readonly,unsafe,volatile,async:error

# Code style defaults
csharp_using_directive_placement = outside_namespace:suggestion
csharp_prefer_braces = true:silent
csharp_preserve_single_line_blocks = true:none
csharp_preserve_single_line_statements = false:none
csharp_prefer_static_local_function = true:suggestion
csharp_prefer_simple_using_statement = true:suggestion
csharp_style_prefer_switch_expression = true:suggestion
csharp_style_prefer_readonly_struct = false:suggestion

# Expression-bodied members
csharp_style_expression_bodied_methods = true:suggestion
csharp_style_expression_bodied_constructors = false:silent
csharp_style_expression_bodied_operators = true:suggestion
csharp_style_expression_bodied_properties = true:suggestion
csharp_style_expression_bodied_indexers = true:suggestion
csharp_style_expression_bodied_accessors = true:suggestion
csharp_style_expression_bodied_lambdas = true:suggestion
csharp_style_expression_bodied_local_functions = true:suggestion

# Pattern matching
csharp_style_prefer_pattern_matching = true:suggestion
csharp_style_pattern_matching_over_is_with_cast_check = true:suggestion
csharp_style_pattern_matching_over_as_with_null_check = true:suggestion
csharp_style_inlined_variable_declaration = true:suggestion

# Expression-level preferences
csharp_prefer_simple_default_expression = true:suggestion
dotnet_style_prefer_simplified_boolean_expressions = true:suggestion
dotnet_style_prefer_collection_expression = when_types_exactly_match:suggestion
dotnet_style_prefer_collection_expression = when_types_loosely_match:suggestion

# Null checking preferences
csharp_style_throw_expression = true:suggestion
csharp_style_conditional_delegate_call = true:suggestion

# Other features
csharp_style_prefer_index_operator = false:suggestion
csharp_style_prefer_range_operator = false:warning
csharp_style_pattern_local_over_anonymous_function = false:none
csharp_style_prefer_primary_constructors = false:suggestion
dotnet_style_prefer_compound_assignment = true:suggestion
csharp_style_implicit_object_creation_when_type_is_apparent = true:suggestion
csharp_style_prefer_tuple_swap = true:suggestion
csharp_style_prefer_utf8_string_literals = true:warning
csharp_style_deconstructed_variable_declaration = true:suggestion
csharp_style_prefer_top_level_statements = false:suggestion
csharp_style_prefer_local_over_anonymous_function = true:suggestion
csharp_style_unused_value_assignment_preference = discard_variable:suggestion
csharp_style_unused_value_expression_statement_preference = discard_variable:suggestion

# Space preferences
csharp_space_after_cast = false
csharp_space_after_colon_in_inheritance_clause = true
csharp_space_after_comma = true
csharp_space_after_dot = false
csharp_space_after_keywords_in_control_flow_statements = true
csharp_space_after_semicolon_in_for_statement = true
csharp_space_around_binary_operators = before_and_after
csharp_space_around_declaration_statements = do_not_ignore
csharp_space_before_colon_in_inheritance_clause = true
csharp_space_before_comma = false
csharp_space_before_dot = false
csharp_space_before_open_square_brackets = false
csharp_space_before_semicolon_in_for_statement = false
csharp_space_between_empty_square_brackets = false
csharp_space_between_method_call_empty_parameter_list_parentheses = false
csharp_space_between_method_call_name_and_opening_parenthesis = false
csharp_space_between_method_call_parameter_list_parentheses = false
csharp_space_between_method_declaration_empty_parameter_list_parentheses = false
csharp_space_between_method_declaration_name_and_open_parenthesis = false
csharp_space_between_method_declaration_parameter_list_parentheses = false
csharp_space_between_parentheses = false
csharp_space_between_square_brackets = false

# Parantheses preferences
dotnet_style_parentheses_in_arithmetic_binary_operators = always_for_clarity:suggestion
dotnet_style_parentheses_in_relational_binary_operators = always_for_clarity:suggestion
dotnet_style_parentheses_in_other_binary_operators = always_for_clarity:suggestion
dotnet_style_parentheses_in_other_operators = always_for_clarity:suggestion

# Types: use keywords instead of BCL types, and permit var only when the type is clear
csharp_style_var_for_built_in_types = false:warning
csharp_style_var_when_type_is_apparent = true:suggestion
csharp_style_var_elsewhere = false:suggestion

csharp_style_namespace_declarations = file_scoped:error

dotnet_analyzer_diagnostic.category-performance.severity = warning

csharp_style_prefer_method_group_conversion = true:silent
dotnet_diagnostic.IDE0005.severity = suggestion
dotnet_diagnostic.IDE0019.severity = suggestion
dotnet_diagnostic.IDE0020.severity = suggestion
dotnet_diagnostic.IDE0051.severity = suggestion
dotnet_diagnostic.IDE0043.severity = suggestion
csharp_style_prefer_null_check_over_type_check = true:suggestion
csharp_style_allow_embedded_statements_on_same_line_experimental = true:silent
csharp_style_allow_blank_lines_between_consecutive_braces_experimental = true:silent
csharp_style_allow_blank_line_after_colon_in_constructor_initializer_experimental = true:silent
csharp_style_allow_blank_line_after_token_in_arrow_expression_clause_experimental = true:silent
csharp_style_allow_blank_line_after_token_in_conditional_expression_experimental = true:silent
csharp_style_prefer_extended_property_pattern = true:suggestion
csharp_style_prefer_not_pattern = true:suggestion

# Xml build files
[*.builds]
indent_size = 2

# Xml files
[*.{xml,stylecop,resx,ruleset}]
indent_size = 2

# Xml config files
[*.{props,targets,config,nuspec}]
indent_size = 2

# Shell scripts
[*.sh]
end_of_line = lf
[*.{cmd, bat}]
end_of_line = crlf
[*.{cs,vb}]
dotnet_style_operator_placement_when_wrapping = beginning_of_line
tab_width = 4
indent_size = 4
end_of_line = crlf

# Xml project files
[*.{csproj,vbproj,vcxproj,vcxproj.filters,proj,nativeproj,locproj}]
indent_size = 4
dotnet_style_coalesce_expression = true:suggestion
dotnet_style_null_propagation = true:suggestion
dotnet_diagnostic.CA1813.severity = warning
dotnet_diagnostic.CA1852.severity = error
dotnet_diagnostic.CA2012.severity = warning
dotnet_diagnostic.IDE0070.severity = suggestion
dotnet_diagnostic.IDE0033.severity = suggestion
dotnet_style_prefer_simplified_boolean_expressions = true:suggestion
dotnet_style_collection_initializer = true:suggestion
dotnet_style_object_initializer = true:suggestion
dotnet_style_prefer_auto_properties = true:suggestion
dotnet_style_prefer_is_null_check_over_reference_equality_method = true:suggestion
dotnet_style_prefer_inferred_tuple_names = true:suggestion
dotnet_style_prefer_inferred_anonymous_type_member_names = true:suggestion
dotnet_style_explicit_tuple_names = true:suggestion
dotnet_style_prefer_conditional_expression_over_return = true:silent
dotnet_style_prefer_conditional_expression_over_assignment = true:silent
dotnet_style_prefer_compound_assignment = true:suggestion
dotnet_style_prefer_simplified_interpolation = true:suggestion
dotnet_style_namespace_match_folder = true:suggestion
dotnet_style_prefer_collection_expression = when_types_loosely_match:suggestion
dotnet_style_predefined_type_for_locals_parameters_members = true:suggestion
dotnet_style_readonly_field = true:suggestion
dotnet_style_predefined_type_for_member_access = true:suggestion
dotnet_style_require_accessibility_modifiers = omit_if_default:silent
dotnet_style_allow_statement_immediately_after_block_experimental = true:silent
dotnet_style_allow_multiple_blank_lines_experimental = true:silent
dotnet_style_parentheses_in_other_binary_operators = always_for_clarity:suggestion
dotnet_style_parentheses_in_arithmetic_binary_operators = always_for_clarity:suggestion
dotnet_code_quality_unused_parameters = all:suggestion
dotnet_style_parentheses_in_relational_binary_operators = always_for_clarity:suggestion
dotnet_style_parentheses_in_other_operators = always_for_clarity:suggestion
dotnet_style_qualification_for_field = false:warning
dotnet_style_qualification_for_event = false:warning
dotnet_style_qualification_for_method = false:warning
dotnet_style_qualification_for_property = false:warning
[*.vb]
dotnet_diagnostic.CA1047.severity = warning