# 每 tick 执行此函数；也可逐行放入重复/连锁命令方块。
# 只处理没有 neo_menu_seen 标签的新雪球，不销毁雪球。
# 每页四项：朝向南/西/北/东选 1/2/3/4；最近玩家仅作为预览对象，不是认证。
execute as @e[type=snowball,tag=!neo_menu_seen] at @s as @p[r=4] if entity @s[rym=-45,ry=45] run tellraw @a {"rawtext":[{"text":"NEO_MENU_SNOW 1 "},{"selector":"@s"}]}
execute as @e[type=snowball,tag=!neo_menu_seen] at @s as @p[r=4] if entity @s[rym=45,ry=135] run tellraw @a {"rawtext":[{"text":"NEO_MENU_SNOW 2 "},{"selector":"@s"}]}
execute as @e[type=snowball,tag=!neo_menu_seen] at @s as @p[r=4] if entity @s[rym=135,ry=180] run tellraw @a {"rawtext":[{"text":"NEO_MENU_SNOW 3 "},{"selector":"@s"}]}
execute as @e[type=snowball,tag=!neo_menu_seen] at @s as @p[r=4] if entity @s[rym=-180,ry=-135] run tellraw @a {"rawtext":[{"text":"NEO_MENU_SNOW 3 "},{"selector":"@s"}]}
execute as @e[type=snowball,tag=!neo_menu_seen] at @s as @p[r=4] if entity @s[rym=-135,ry=-45] run tellraw @a {"rawtext":[{"text":"NEO_MENU_SNOW 4 "},{"selector":"@s"}]}
tag @e[type=snowball,tag=!neo_menu_seen] add neo_menu_seen
