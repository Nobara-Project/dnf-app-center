"""Search with more than one word (#17).

`dnf search libre office` lists every package that matches both words, but the
App Center looked for the whole text "libre office" and found nothing. Each
word now has to match somewhere, in any order and any field, while results
that contain the text exactly as typed keep their old order at the top.
Run from the repository root: python3 -m unittest discover -s tests
"""
import unittest

from appcenter import dnf_backend, ui
from appcenter.models import AppEntry


def package(name, summary):
    return AppEntry(appstream_id=f"pkg:{name}", name=name, summary=summary, description=summary,
                    pkg_names=[name], kind="PACKAGE")


PACKAGES = [
    package("libre", "Library to read office documents"),
    package("libreoffice", "Free Software Productivity Suite"),
    package("libreoffice-calc", "LibreOffice Spreadsheet Application"),
    package("libreoffice-langpack-pl", "Polish language pack for LibreOffice"),
    package("librecad", "2D CAD drawing application"),
    package("onlyoffice-desktopeditors", "Office suite"),
    package("falkon", "Modern web browser"),
    package("webbrowser-chooser", "Pick which browser opens links"),
]

CALC = AppEntry("org.libreoffice.LibreOffice.calc", "LibreOffice Calc", "Spreadsheet application",
                "Calc is the spreadsheet program you've always needed.",
                pkg_names=["libreoffice-calc"], categories=["Office", "Spreadsheet"], installed=True)
FIREFOX = AppEntry("org.mozilla.firefox", "Firefox", "Web Browser", "Browse the web.",
                   pkg_names=["firefox"], categories=["Network", "WebBrowser"], installed=True)
CHOOSER = AppEntry("io.example.BrowserChooser", "Browser Chooser", "Pick which browser opens links",
                   "Asks which browser to use when you click a link.",
                   pkg_names=["webbrowser-chooser"], categories=["Utility"])
KATE = AppEntry("org.kde.kate", "Kate", "Advanced text editor", "A multi-document editor.",
                pkg_names=["kate"], categories=["Utility", "TextEditor"])
APPS = [CALC, FIREFOX, CHOOSER, KATE]


def backend_with(packages):
    backend = dnf_backend.DnfBackend.__new__(dnf_backend.DnfBackend)
    backend._package_search_cache = {app.name: app for app in packages}
    return backend


class FakeWindow:
    """The state MainWindow's page filter reads; its methods come from MainWindow."""

    def __init__(self, apps, **state):
        self.apps = apps
        self.backend = None
        self._data_revision = 0
        self._page_items_cache = {}
        self.current_group = "system"
        self.current_page = "installed"
        self.current_subcategory = None
        self.current_search_text = ""
        self.current_category_filter_text = ""
        self.current_repo_filter = "__all__"
        self.__dict__.update(state)

    def __getattr__(self, name):
        return getattr(ui.MainWindow, name).__get__(self)


def names(apps):
    return [app.name for app in apps]


class PackageSearchTests(unittest.TestCase):
    def search(self, query):
        return names(backend_with(PACKAGES).search_packages(query))

    def test_every_word_has_to_match(self):
        cases = {
            "libre office": ["libreoffice", "libreoffice-calc", "libreoffice-langpack-pl", "libre"],
            "office libre": ["libreoffice", "libreoffice-calc", "libreoffice-langpack-pl", "libre"],
            "  LIBRE\tOffice  ": ["libreoffice", "libreoffice-calc", "libreoffice-langpack-pl", "libre"],
            "polish libre": ["libreoffice-langpack-pl"],
            "libre browser": [],
        }
        for query, expected in cases.items():
            with self.subTest(query=query):
                self.assertEqual(self.search(query), expected)

    def test_one_word_search_is_unchanged(self):
        self.assertEqual(self.search("libre"),
                         ["libre", "librecad", "libreoffice", "libreoffice-calc", "libreoffice-langpack-pl"])

    def test_results_rank_by_their_worst_matching_word(self):
        # "libre" is an exact name match for one word, but "office" is only in
        # its summary, so it ranks below names that contain both words.
        self.assertEqual(self.search("libre office")[-1:], ["libre"])

    def test_exact_text_matches_come_before_separate_word_matches(self):
        # "webbrowser-chooser" matches both words in its name, which would
        # outrank falkon's summary match if only the words counted.
        self.assertEqual(self.search("web browser"), ["falkon", "webbrowser-chooser"])

    def test_blank_query_finds_nothing(self):
        self.assertEqual(self.search(" \t "), [])


class SearchPageTests(unittest.TestCase):
    def search(self, query):
        window = FakeWindow(APPS, current_group="categories", current_page="office",
                            current_search_text=query.strip())
        return names(window._filtered_apps_for_current_page())

    def test_every_word_has_to_match(self):
        cases = {
            "libre office": ["LibreOffice Calc"],
            "office libre": ["LibreOffice Calc"],
            "LIBRE   office": ["LibreOffice Calc"],
            "libre spreadsheet": ["LibreOffice Calc"],
            "libre firefox": [],
        }
        for query, expected in cases.items():
            with self.subTest(query=query):
                self.assertEqual(self.search(query), expected)

    def test_one_word_search_is_unchanged(self):
        self.assertEqual(self.search("browser"), ["Browser Chooser", "Firefox"])

    def test_exact_text_matches_come_before_separate_word_matches(self):
        self.assertEqual(self.search("web browser"), ["Firefox", "Browser Chooser"])

    def test_package_results_from_the_backend_use_every_word(self):
        window = FakeWindow([KATE], backend=backend_with(PACKAGES), current_group="categories",
                            current_page="office", current_search_text="libre office")
        self.assertEqual(names(window._filtered_apps_for_current_page()),
                         ["libreoffice", "libreoffice-calc", "libreoffice-langpack-pl", "libre"])


class PageFilterTests(unittest.TestCase):
    def test_installed_page_filter_matches_every_word(self):
        window = FakeWindow(APPS, current_category_filter_text="libre calc")
        self.assertEqual(names(window._filtered_apps_for_current_page()), ["LibreOffice Calc"])


if __name__ == "__main__":
    unittest.main()
