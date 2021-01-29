import json
import pandas as pd
from collections import defaultdict
import mongoengine as db
from flask_mongoengine.wtf import model_form
from flask_mongoengine.wtf.orm import ModelConverter
from wtforms import fields as f, validators
from random import shuffle

CHOICES1 = (
    "Never",
    "Rarely",
    "Sometimes",
    "Often",
    "Always",
)
CHOICES2 = ("Stayed the same", "Decreased", "Increased")
CHOICES3 = ("Stayed the same", "Grown looser", "Grown tighter")
CHOICES4 = ("Very much", "Quite a bit", "Somewhat", "A little bit", "Not at all")
CHOICES5 = CHOICES4 + ("I have not had sexual activity in the past 30 days",)


def echoices(choices, rev=False):
    f = reversed if rev else lambda x: x
    return list(enumerate(f(choices)))


class Energy(db.EmbeddedDocument):
    meta = {"strict": False}
    tired = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt tired...",
    )
    exhaustion = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I experienced extreme exhaustion...",
    )
    energy = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I ran out of energy...",
    )
    work = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, my fatigue limited me at work...",
        help_text="(include work at home)",
    )
    think = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I was too tired to think clearly...",
    )
    bathe = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I was too tired to take a bath or shower...",
    )
    exercise = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I was too tired to exercise strenuously...",
    )


class Sleep(db.EmbeddedDocument):
    meta = {"strict": False}
    falling = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I had difficulty falling asleep...",
    )
    staying = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I had difficulty staying asleep...",
    )
    eight = db.IntField(
        choices=echoices(CHOICES1, rev=True),
        verbose_name="In the past seven days, I got eight hours of sleep...",
    )
    refreshing = db.IntField(
        choices=echoices(CHOICES1, rev=True),
        verbose_name="In the past seven days, my sleep was refreshing...",
    )


class Digestion(db.EmbeddedDocument):
    meta = {"strict": False}
    gas = db.IntField(
        choices=echoices(CHOICES1), verbose_name="In the past seven days, I had gas..."
    )
    bloated = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I was bloated...",
    )
    constipated = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I went more than a day without defecation...",
    )
    diarrhoea = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I had diarrhoea...",
    )
    rumbled = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, my stomach rumbled...",
    )
    sweet = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I had sweet cravings...",
    )
    fat = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I had cravings for fat...",
    )
    satisfy = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, eating didn't satisfy my hunger...",
    )
    tired = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt tired after eating...",
    )
    hours = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I couldn't go more than a few hours without eating...",
    )
    shaky = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt shaky in between meals...",
    )
    irritable = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt irritable in between meals...",
    )


class Body(db.EmbeddedDocument):
    meta = {"strict": False}
    weight = db.IntField(
        choices=echoices(CHOICES2), verbose_name="In the past 30 days, my weight has..."
    )
    pants = db.IntField(
        choices=echoices(CHOICES3), verbose_name="In the past 30 days, my pants have..."
    )
    happy1 = db.IntField(
        choices=echoices(CHOICES4),
        verbose_name="In the past 30 days, I have been happy with my body weight...",
    )
    happy2 = db.IntField(
        choices=echoices(CHOICES4),
        verbose_name="In the past 30 days, I have been happy with my body composition (fatness)...",
        help_text="(fatness)",
    )


class Emotion(db.EmbeddedDocument):
    meta = {"strict": False}
    worthless = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I have felt worthless...",
    )
    forward = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I had nothing to look forward to...",
    )
    helpless = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt helpless...",
    )
    sad = db.IntField(
        choices=echoices(CHOICES1), verbose_name="In the past seven days, I felt sad..."
    )
    failure = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt like a failure...",
    )
    depressed = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt depressed...",
    )
    unhappy = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt unhappy...",
    )
    hopeless = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt hopeless...",
    )
    fearful = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt fearful...",
    )
    anxious = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt anxious...",
    )
    worried = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt worried...",
    )
    anxiety = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt it hard to focus on anything other than my anxiety...",
    )
    nervous = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt nervous...",
    )
    uneasy = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt uneasy...",
    )
    tense = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt tense...",
    )
    irritated = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I was irritated more than people knew...",
    )
    angry = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt angry...",
    )
    explode = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt ready to explode...",
    )
    grouchy = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt grouchy...",
    )
    annoyed = db.IntField(
        choices=echoices(CHOICES1),
        verbose_name="In the past seven days, I felt annoyed...",
    )


class Sex(db.EmbeddedDocument):
    meta = {"strict": False}
    interested = db.IntField(
        choices=echoices(CHOICES4),
        verbose_name="In the past 30 days, I have been interested in sexual activity...",
    )
    enjoyed = db.IntField(
        choices=echoices(CHOICES5),
        verbose_name="In the past 30 days when I had sexual activity, I enjoyed it...",
    )


class Isolation(db.EmbeddedDocument):
    meta = {"strict": False}
    feel = db.IntField(
        choices=echoices(CHOICES1, rev=True), verbose_name="I feel isolated..."
    )
    talk = db.IntField(
        choices=echoices(CHOICES1, rev=True),
        verbose_name="There are people I can talk to...",
    )
    friends = db.IntField(
        choices=echoices(CHOICES1, rev=True),
        verbose_name="I feel part of a group of friends...",
    )


class Challenges(db.EmbeddedDocument):
    meta = {"strict": False, "close_ended": False}
    when = db.StringField(
        verbose_name="When it comes to maintaining health and fitness, what’s the single biggest challenge you’ve been struggling with?",
        help_text="(Please be as detailed and specific as possible)",
    )


class Tally(dict):
    def get_data(self):
        return sorted(
            [
                {"name": key, "y": sum(self[key].values()), "drilldown": key}
                for (key, val) in self.items()
            ],
            key=lambda x: x["name"],
        )

    def get_drilldown(self):
        return sorted([
            {
                "name": key,
                "id": key,
                "data": [[key_, val_] for (key_, val_) in val.items()],
            }
            for (key, val) in self.items()
        ], key=lambda x: x["name"])

    def sum_cat(self):
        return dict([(key, sum(val.values())) for (key, val) in self.items()])


class HAQ(db.Document):
    meta = {"allow_inheritance": True}
    rptid = db.StringField(required=True)
    energy = db.EmbeddedDocumentField(Energy)
    sleep = db.EmbeddedDocumentField(Sleep)
    digestion = db.EmbeddedDocumentField(Digestion)
    body = db.EmbeddedDocumentField(Body)
    emotion = db.EmbeddedDocumentField(Emotion)
    sex = db.EmbeddedDocumentField(Sex)
    isolation = db.EmbeddedDocumentField(Isolation)
    challenges = db.EmbeddedDocumentField(Challenges)

    def __init__(self, rptid, *args, **values):
        super().__init__(*args, **values)
        for (attr, cls) in (
            ("energy", Energy),
            ("sleep", Sleep),
            ("digestion", Digestion),
            ("body", Body),
            ("emotion", Emotion),
            ("sex", Sex),
            ("isolation", Isolation),
            ("challenges", Challenges),
        ):
            obj = getattr(self, attr, None)
            if obj is None:
                setattr(self, attr, cls())

    @classmethod
    def get(cls, rptid, default=None):
        if default is None:
            default = cls(rptid=rptid)
        try:
            return cls.objects.get(rptid=rptid)
        except HAQ.DoesNotExist:
            return default

    @classmethod
    def get_verbose_name(cls, attr1, attr2, scrub=True):
        cls = getattr(cls, attr1).document_type
        vname = getattr(cls, attr2).verbose_name
        if scrub:
            vname = vname.replace("In the past seven days, ", "").capitalize()
        return vname

    @classmethod
    def get_cls_name(cls, attr1):
        return getattr(cls, attr1).document_type._class_name

    @classmethod
    def is_close_ended(cls, attr):
        field = getattr(cls, attr)
        if type(field) is not db.EmbeddedDocumentField:
            return False
        return field.document_type._meta.get("close_ended", True)

    def get_tally(self):
        tally = Tally()
        for attr in self:
            if HAQ.is_close_ended(attr):
                obj = getattr(self, attr)
                subtally = {}
                for attr_ in obj:
                    val = getattr(obj, attr_)
                    if type(val) is not int:
                        continue
                    subtally[HAQ.get_verbose_name(attr, attr_)] = val
                tally[HAQ.get_cls_name(attr)] = subtally
        return tally

    def get_val(self, key):
        (k, k_) = key.split(".")
        return getattr(getattr(self, k), f"get_{k_}_display")()

    def get_df(self, qmap):
        d = {}
        for k, v in json.loads(self.to_json()).items():
            if k.startswith("_"):
                continue
            for k1, v1 in v.items():
                k2 = f"{k}.{k1}"
                k3 = qmap.get(k2)
                if k3 is not None:
                    d[k3] = self.get_val(k2)
        return pd.DataFrame.from_dict([d])


def get_haq_combined_data(reports):
    dd = defaultdict(list)
    for (rptid, args) in reports.sorted():
        start = args["date"] * 1000  # ms for JS
        for (key, val) in HAQ.get(rptid).get_tally().sum_cat().items():
            dd[key].append([start, val])
    return dd


def get_haq_fields(form, shuffle_=True):
    fields = []

    def get_fields_(form, fields):
        for field in form:
            if type(field) is f.core.FormField:
                get_fields_(getattr(form, field.name), fields)
            elif (
                type(field) is not f.simple.TextAreaField
                and type(field) is not f.simple.StringField
            ):
                fields.append(field)

    get_fields_(form, fields)
    if shuffle_:
        shuffle(fields)
    return fields


HAQForm = model_form(HAQ)  # , converter=MyModelConverter())
