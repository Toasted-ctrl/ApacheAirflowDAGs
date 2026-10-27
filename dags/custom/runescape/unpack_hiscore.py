from dataclasses import dataclass


@dataclass(frozen=True)
class HiscoreItem:
    is_skill: bool
    name: str
    progression: str
    progression_points: int
    level: int | None
    rank: int


def unpack_hiscore_item(is_skill: bool, listing: list, name: str) -> HiscoreItem:
    """Returns a HiscoreItem object for the provided listing."""

    return HiscoreItem(
        is_skill=is_skill,
        name=name,
        progression="Experience" if is_skill else "Points",
        progression_points=listing[2] if is_skill else listing[1],
        level=listing[1] if is_skill else None,
        rank=listing[0],
    )


def unpack_hiscore_entry(input: str) -> list[HiscoreItem]:
    """Unpacks the hiscore body of text returned from the RuneScape Hiscore API.
    Returns a list of HiscoreItem objects."""

    hiscore_items: list = [
        "Overall",
        "Attack",
        "Defence",
        "Strength",
        "Constitution",
        "Ranged",
        "Prayer",
        "Magic",
        "Cooking",
        "Woodcutting",
        "Fletching",
        "Fishing",
        "Firemaking",
        "Crafting",
        "Smithing",
        "Mining",
        "Herblore",
        "Agility",
        "Thieving",
        "Slayer",
        "Farming",
        "Runecrafting",
        "Hunting",
        "Construction",
        "Summoning",
        "Dungeoneering",
        "Divination",
        "Invention",
        "Archaeology",
        "Necromancy",
        "Bounty Hunter",
        "B.H. Rogues",
        "Dominion Tower",
        "The Crucible",
        "Castle Wars games",
        "B.A. Attackers",
        "B.A. Defenders",
        "B.A. Collectors",
        "B.A. Healers",
        "Duel Tournament",
        "Mobilising Armies",
        "Conquest",
        "Fist of Guthix",
        "GG: Athletics",
        "GG: Resource Race",
        "WE2: Armadyl Lifetime Contribution",
        "WE2: Bandos Lifetime Contribution",
        "WE2: Armadyl PvP kills",
        "WE2: Bandos PvP kills",
        "Heist Guard Level",
        "Heist Robber Level",
        "CFP: 5 game average",
        "AF15: Cow Tipping",
        "AF15: Rats killed after the miniquest",
        "RuneScore",
        "Clue Scrolls Easy",
        "Clue Scrolls Medium",
        "Clue Scrolls Hard",
        "Clue Scrolls Elite",
        "Clue Scrolls Master",
        "League Points",
    ]

    m = []
    listings = input.split("\n")
    for idx, line in enumerate(listings):
        listing = line.split(",")
        if idx < 61:
            m.append(
                unpack_hiscore_item(
                    is_skill=idx < 30, listing=listing, name=hiscore_items[idx]
                )
            )

    return m
